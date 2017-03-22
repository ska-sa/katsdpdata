#!/usr/bin/env python

"""Capture L0 visibilities from a SPEAD stream and write to HDF5 file. When
the file is closed, metadata is also extracted from the telescope state and
written to the file. This process lives across multiple observations and
hence multiple HDF5 files.

The status sensor has the following states:

  - `idle`: data is not being captured
  - `capturing`: data is being captured
  - `ready`: CBF data stream has finished, waiting for capture_done request
  - `finalising`: metadata is being written to file
"""

from __future__ import print_function, division
import spead2
import spead2.recv
import katsdptelstate
import time
import os.path
import os
import sys
import threading
import logging
import Queue
import numpy as np
import signal
import manhole
from katcp import DeviceServer, Sensor
from katcp.kattypes import request, return_reply, Str
from katsdpfilewriter import telescope_model, ar1_model, file_writer


#: Bytes free at which a running capture will be stopped
FREE_DISK_THRESHOLD_STOP = 2 * 1024**3
#: Bytes free at which a new capture will be refused
FREE_DISK_THRESHOLD_START = 3 * 1024**3


class FileWriterServer(DeviceServer):
    VERSION_INFO = ("sdp-file-writer", 0, 1)
    BUILD_INFO = ("sdp-file-writer", 0, 1, "rc1")

    def __init__(self, logger, l0_spectral_endpoints, file_base, antenna_mask, telstate, *args, **kwargs):
        super(FileWriterServer, self).__init__(*args, logger=logger, **kwargs)
        self._file_base = file_base
        self._endpoints = l0_spectral_endpoints
        self._capture_thread = None
        self._telstate = telstate
        self._model = ar1_model.create_model(antenna_mask=antenna_mask)
        self._file_obj = None
        self._start_timestamp = None
        self._rx = None

    def setup_sensors(self):
        self._status_sensor = Sensor.string(
                "status", "The current status of the capture process", "", "idle")
        self.add_sensor(self._status_sensor)
        self._device_status_sensor = Sensor.string(
                "device-status", "Health sensor", "", "ok")
        self.add_sensor(self._device_status_sensor)
        self._filename_sensor = Sensor.string(
                "filename", "Final name for file being captured", "")
        self.add_sensor(self._filename_sensor)
        self._input_dumps_sensor = Sensor.integer(
                "input-dumps-total", "Number of input dumps captured.", "", default=0)
        self.add_sensor(self._input_dumps_sensor)
        self._input_bytes_sensor = Sensor.integer(
                "input-bytes-total", "Number of payload bytes received in this session.", "B", default=0)
        self.add_sensor(self._input_bytes_sensor)
        self._disk_free_sensor = Sensor.float(
                "disk-free", "Free disk space in bytes on target device for this file.", "B")
        self.add_sensor(self._disk_free_sensor)

    def _do_capture(self, file_obj):
        """Capture a stream from SPEAD and write to file. This is run in a
        separate thread.

        Parameters
        ----------
        file_obj : :class:`filewriter.File`
            Output file object
        """
        timestamps = []
        n_dumps = 0
        n_bytes = 0
        self._input_dumps_sensor.set_value(n_dumps)
        self._input_bytes_sensor.set_value(n_bytes)
        loop_time = time.time()
        free_space = file_obj.free_space()
        self._disk_free_sensor.set_value(free_space)
        # status to report once the capture stops
        end_status = "ready"
        try:
            ig = spead2.ItemGroup()
            for heap in self._rx:
                updated = ig.update(heap)
                if 'timestamp' in updated:
                    vis_data = ig['correlator_data'].value
                    flags = ig['flags'].value
                    try:
                        weights = ig['weights'].value
                    except KeyError:
                        weights = None
                    try:
                        weights_channel = ig['weights_channel'].value
                    except KeyError:
                        weights_channel = None
                    file_obj.add_data_frame(vis_data, flags, weights, weights_channel)
                    timestamps.append(ig['timestamp'].value)
                    n_dumps += 1
                    n_bytes += vis_data.nbytes + flags.nbytes
                    self._input_dumps_sensor.set_value(n_dumps)
                    self._input_bytes_sensor.set_value(n_bytes)
                free_space = file_obj.free_space()
                self._disk_free_sensor.set_value(free_space)
                if free_space < FREE_DISK_THRESHOLD_STOP:
                    self._logger.error('Stopping capture because only %d bytes left on disk',
                                       free_space)
                    self._rx.stop()
                    end_status = "disk-full"
                    self._device_status_sensor.set_value("fail", "error")
        except Exception as err:
            self._logger.error(err)
            end_status = "error"
        finally:
            self._status_sensor.set_value(end_status)
            self._input_bytes_sensor.set_value(0)
            self._input_dumps_sensor.set_value(0)
            # Timestamps in the SPEAD stream are relative to sync_time
            if not timestamps:
                self._logger.warning("H5 file contains no data and hence no timestamps")
            else:
                timestamps = np.array(timestamps) + self._telstate.cbf_sync_time
                file_obj.set_timestamps(timestamps)
                self._logger.info('Set %d timestamps', len(timestamps))

    @request()
    @return_reply(Str())
    def request_capture_init(self, req):
        """Start listening for L0 data and write it to HDF5 file."""
        if self._capture_thread is not None:
            self._logger.info("Ignoring capture_init because already capturing")
            return ("fail", "Already capturing")
        timestamp = time.time()
        self._final_filename = os.path.join(
                self._file_base, "{0}.h5".format(int(timestamp)))
        self._stage_filename = os.path.join(
                self._file_base, "{0}.writing.h5".format(int(timestamp)))
        try:
            stat = os.statvfs(os.path.dirname(self._stage_filename))
        except OSError:
            self._logger.warn("Failed to check free disk space, continuing anyway")
        else:
            free_space = stat.f_bsize * stat.f_bavail
            if free_space < FREE_DISK_THRESHOLD_START:
                self._logger.error("Insufficient disk space to start capture (%d < %d)",
                                  free_space, FREE_DISK_THRESHOLD_START)
                self._device_status_sensor.set_value("fail", "error")
                return ("fail", "Disk too full (only {:.2f} GiB free)".format(free_space / 1024**3))
        self._device_status_sensor.set_value("ok")
        self._filename_sensor.set_value(self._final_filename)
        self._status_sensor.set_value("capturing")
        self._input_dumps_sensor.set_value(0)
        self._input_bytes_sensor.set_value(0)
        self._file_obj = file_writer.File(self._stage_filename)
        self._start_timestamp = timestamp
        self._rx = spead2.recv.Stream(spead2.ThreadPool(), bug_compat=spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=2, ring_heaps=2)
         # as a temporary fix we try and allocate memory pools sized to fit the maximum expected
         # heap size for AR1 which is 16 antennas, 32k channels, 9 bytes per vis
        l0_heap_size = 16 * 17 * 2 * 32768 * 9
        memory_pool = spead2.MemoryPool(l0_heap_size, l0_heap_size+4096, 8, 8)
        self._rx.set_memory_pool(memory_pool)

        for endpoint in self._endpoints:
            self._rx.add_udp_reader(endpoint.port, bind_hostname=endpoint.host, buffer_size=l0_heap_size)
        self._capture_thread = threading.Thread(
                target=self._do_capture, name='capture', args=(self._file_obj,))
        self._capture_thread.start()
        self._logger.info("Starting capture to %s", self._stage_filename)
        return ("ok", "Capture initialised to {0}".format(self._stage_filename))

    @request()
    @return_reply(Str())
    def request_capture_done(self, req):
        """Stop capturing and close the HDF5 file, if it is not already done."""
        if self._capture_thread is None:
            self._logger.info("Ignoring capture_done because already explicitly stopped")
        return self.capture_done()

    def capture_done(self):
        """Implementation of :meth:`request_capture_done`, split out to allow it
        to be called on `KeyboardInterrupt`.
        """
        if self._capture_thread is None:
            return ("fail", "Not capturing")
        self._rx.stop()
        self._capture_thread.join()
        self._capture_thread = None
        self._rx = None
        self._logger.info("Joined capture thread")

        self._status_sensor.set_value("finalising")
        self._file_obj.set_metadata(telescope_model.TelstateModelData(
                self._model, self._telstate, self._start_timestamp))
        self._file_obj.close()
        self._file_obj = None
        self._start_timestamp = None
        self._logger.info("Finalised file")

        # File is now closed, so rename it
        try:
            os.rename(self._stage_filename, self._final_filename)
            result = ("ok", "File renamed to {0}".format(self._final_filename))
        except OSError as e:
            logger.error("Failed to rename output file %s to %s",
                         self._stage_filename, self._final_filename, exc_info=True)
            result = ("fail", "Failed to rename output file from {0} to {1}.".format(
                self._stage_filename, self._final_filename))
        self._status_sensor.set_value("idle")
        return result

def comma_list(type_):
    """Return a function which splits a string on commas and converts each element to
    `type_`."""

    def convert(arg):
        return [type_(x) for x in arg.split(',')]
    return convert

def main():
    if len(logging.root.handlers) > 0: logging.root.removeHandler(logging.root.handlers[0])
    formatter = logging.Formatter("%(asctime)s.%(msecs)03dZ - %(filename)s:%(lineno)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logging.root.addHandler(sh)

    logger = logging.getLogger("katsdpfilewriter")
    logger.setLevel(logging.INFO)
    logging.getLogger('spead2').setLevel(logging.WARNING)

    parser = katsdptelstate.ArgumentParser()
    parser.add_argument('--l0-spectral-spead', type=katsdptelstate.endpoint.endpoint_list_parser(7200), default=':7200', help='source port/multicast groups for spectral L0 input. [default=%(default)s]', metavar='ENDPOINTS')
    parser.add_argument('--file-base', default='.', type=str, help='base directory into which to write HDF5 files. [default=%(default)s]', metavar='DIR')
    parser.add_argument('--antenna-mask', type=comma_list(str), default='', help='List of antennas to store in the telescope model. [default=%(default)s]')
    parser.add_argument('-p', '--port', dest='port', type=int, default=2046, metavar='N', help='katcp host port. [default=%(default)s]')
    parser.add_argument('-a', '--host', dest='host', type=str, default="", metavar='HOST', help='katcp host address. [default=all hosts]')
    parser.set_defaults(telstate='localhost')
    args = parser.parse_args()
    if not os.access(args.file_base, os.W_OK):
        logger.error('Target directory (%s) is not writable', args.file_base)
        sys.exit(1)

    restart_queue = Queue.Queue()
    server = FileWriterServer(logger, args.l0_spectral_spead, args.file_base, args.antenna_mask, args.telstate,
                              host=args.host, port=args.port)
    server.set_restart_queue(restart_queue)
    server.start()
    logger.info("Started file writer server.")


    manhole.install(oneshot_on='USR1', locals={'server':server, 'args':args})
     # allow remote debug connections and expose server and args

    def graceful_exit(_signo=None, _stack_frame=None):
        logger.info("Exiting filewriter on SIGTERM")
        os.kill(os.getpid(), signal.SIGINT)
         # rely on the interrupt handler around the katcp device server
         # to peform graceful shutdown. this preserves the command
         # line Ctrl-C shutdown.

    signal.signal(signal.SIGTERM, graceful_exit)
     # mostly needed for Docker use since this process runs as PID 1
     # and does not get passed sigterm unless it has a custom listener

    try:
        while True:
            try:
                device = restart_queue.get(timeout=0.5)
            except Queue.Empty:
                device = None
            if device is not None:
                logger.info("Stopping")
                device.capture_done()
                device.stop()
                device.join()
                logger.info("Restarting")
                device.start()
                logger.info("Started")
    except KeyboardInterrupt:
        logger.info("Shutting down file_writer server...")
        logger.info("Activity logging stopped")
        server.capture_done()
        server.stop()
        server.join()

if __name__ == '__main__':
    main()
