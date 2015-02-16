#!/usr/bin/env python

"""Capture L0 visibilities from a SPEAD stream and write to HDF5 file. When
the file is closed, metadata is also extracted from the telescope state and
written to the file. This process lives across multiple observations and
hence multiple HDF5 files.

The status sensor has the following states:

  - `idle`: data is not being captured
  - `capturing`: data is being captured
  - `finalising`: file is being finalised
"""

from __future__ import print_function, division
import spead64_48 as spead
import katsdptelstate
import time
import os.path
import os
import socket
import threading
import logging
import Queue
import numpy as np
from katcp import DeviceServer, Sensor
from katcp.kattypes import request, return_reply, Str
from katsdpfilewriter import rts_model, file_writer

class FileWriterServer(DeviceServer):
    VERSION_INFO = ("sdp-file-writer", 0, 1)
    BUILD_INFO = ("sdp-file-writer", 0, 1, "rc1")

    def __init__(self, logger, l0_spectral_endpoints, file_base, telstate, *args, **kwargs):
        super(FileWriterServer, self).__init__(*args, logger=logger, **kwargs)
        self._file_base = file_base
        self._endpoints = l0_spectral_endpoints
        self._capture_thread = None
        self._telstate = telstate
        self._model = rts_model.create_model()

    def setup_sensors(self):
        self._status_sensor = Sensor.string("status", "The current status of the capture process", "", "idle")
        self.add_sensor(self._status_sensor)
        self._filename_sensor = Sensor.string("filename", "Final name for file being captured", "")
        self.add_sensor(self._filename_sensor)
        self._dumps_sensor = Sensor.integer("dumps", "Number of L0 dumps captured", "", [0, 2**63], 0)
        self.add_sensor(self._dumps_sensor)

    def _multicast_socket(self):
        """Returns a socket that is subscribed to any necessary multicast groups."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        for endpoint in self._endpoints:
            if endpoint.multicast_subscribe(sock):
                self._logger.info("Subscribing to multicast address {0}".format(endpoint.host))
            elif endpoint.host != '':
                self._logger.warning("Ignoring non-multicast address {0}".format(endpoint.host))
        return sock

    def _get_attribute(self, attribute):
        return self._telstate.get(attribute.full_name)

    def _get_sensor(self, sensor, start_timestamp):
        try:
            values = self._telstate.get_range(sensor.full_name, start_timestamp, np.inf)
        except KeyError:
            return None
        if values is None:
            return None
        # Reorder fields, and insert a status of 'nominal' since we don't get
        # any status information from the telescope state
        return [(ts, value, 'nominal') for (value, ts) in values]

    def _do_capture(self, file_obj, start_timestamp):
        """Capture a stream from SPEAD and write to file. This is run in a
        separate thread.

        Parameters
        ----------
        file_obj : :class:`filewriter.File`
            Output file object
        """
        timestamps = []
        sock = self._multicast_socket()
        n_dumps = 0
        try:
            rx = spead.TransportUDPrx(self._endpoints[0].port, pkt_count=1024, buffer_size=51200000)
            ig = spead.ItemGroup()
            for heap in spead.iterheaps(rx):
                ig.update(heap)
                file_obj.add_data_frame(ig['correlator_data'], ig['flags'])
                timestamps.append(ig['timestamp'])
                n_dumps += 1
                self._dumps_sensor.set_value(n_dumps)
        finally:
            self._status_sensor.set_value("finalising")
            sock.close()
            # Timestamps in the SPEAD stream are relative to sync_time
            if not timestamps:
                self._logger.warning("H5 file contains no data and hence no timestamps")
            else:
                timestamps = np.array(timestamps) + self._telstate.cbf_sync_time
                file_obj.set_timestamps(timestamps)
                self._logger.info('Set %d timestamps', len(timestamps))
            file_obj.set_metadata(self._model, self._get_attribute,
                    lambda sensor: self._get_sensor(sensor, start_timestamp))
            file_obj.close()

    @request()
    @return_reply(Str())
    def request_capture_init(self, req):
        """Start listening for L0 data and write it to HDF5 file."""
        if self._capture_thread is not None:
            self._logger.info("Ignoring capture_init because already capturing")
            return ("fail", "Already capturing")
        timestamp = time.time()
        self._final_filename = os.path.join(self._file_base, "{0}.h5".format(int(timestamp)))
        self._stage_filename = os.path.join(self._file_base, "{0}.writing.h5".format(int(timestamp)))
        self._filename_sensor.set_value(self._final_filename)
        self._status_sensor.set_value("capturing")
        self._dumps_sensor.set_value(0)
        f = file_writer.File(self._stage_filename)
        self._capture_thread = threading.Thread(target=self._do_capture, name='capture', args=(f, timestamp))
        self._capture_thread.start()
        self._logger.info("Starting capture to %s", self._stage_filename)
        return ("ok", "Capture initialised to {0}".format(self._stage_filename))

    @request()
    @return_reply(Str())
    def request_capture_done(self, req):
        """Stop capturing and close the HDF5 file, if it is not already done."""
        return self.capture_done()

    def capture_done(self):
        """Implementation of :meth:`request_capture_done`, split out to allow it
        to be called on `KeyboardInterrupt`.
        """
        if self._capture_thread is None:
            self._logger.info("Ignoring capture_done because already explicitly stopped")
            return ("fail", "Not capturing")
        # Nasty hack until PySPEAD has a way to interrupt an iterheaps: send an
        # end-of-stream packet to ourself, and keep doing it until it is received
        tx = spead.Transmitter(spead.TransportUDPtx('localhost', self._endpoints[0].port))
        while self._capture_thread.is_alive():
            tx.send_halt()
            time.sleep(0.1)
        self._capture_thread.join()
        self._capture_thread = None
        self._status_sensor.set_value("idle")

        # File is now closed, so rename it
        try:
            os.rename(self._stage_filename, self._final_filename)
        except OSError as e:
            logger.error("Failed to rename output file %s to %s".format(self._stage_filename, self._final_filename, exc_info=True))
            return ("fail","Failed to rename output file from {0} to {1}.".format(self._stage_filename, self._final_filename))
        return ("ok", "File renamed to {0}".format(self._final_filename))

def main():
    logging.basicConfig()
    logger = logging.getLogger("katsdpresearch.file_writer")
    logger.setLevel(logging.INFO)
    spead.logger.setLevel(logging.WARNING)

    parser = katsdptelstate.ArgumentParser()
    parser.add_argument('--l0-spectral-spead', type=katsdptelstate.endpoint.endpoint_list_parser(7200), default=':7200', help='source port/multicast groups for spectral L0 input. [default=%(default)s]', metavar='ENDPOINTS')
    parser.add_argument('--file-base', default='.', type=str, help='base directory into which to write HDF5 files. [default=%(default)s]', metavar='DIR')
    parser.add_argument('-p', '--port', dest='port', type=int, default=2046, metavar='N', help='katcp host port. [default=%(default)s]')
    parser.add_argument('-a', '--host', dest='host', type=str, default="", metavar='HOST', help='katcp host address. [default=all hosts]')
    parser.set_defaults(telstate='localhost')
    args = parser.parse_args()

    restart_queue = Queue.Queue()
    server = FileWriterServer(logger, args.l0_spectral_spead, args.file_base, args.telstate,
            host=args.host, port=args.port)
    server.set_restart_queue(restart_queue)
    server.start()
    logger.info("Started file writer server.")
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
