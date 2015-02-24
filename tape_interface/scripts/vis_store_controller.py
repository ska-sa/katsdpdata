#!/usr/bin/env python
from katcp import DeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Float, Timestamp, Discrete, Int, request, return_reply)

import threading
import time
import random

import tape_archive

import signal
import sys

server_host = "192.168.6.233"
server_port = 5000

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

class tape_katcp_server(DeviceServer):

    VERSION_INFO = ("tape_katcp_interface", 1, 0)
    BUILD_INFO = ("tape_katcp_interface", 0, 1, "")


    def setup_sensors(self):
        """Setup some server sensors."""

        self.buffer_dirs=["/var/kat/data/tape_buffer1", "/var/kat/data/tape_buffer2"]
        self.buffer_index = 0

        self._buffer_dir = Sensor.string("buffer_dir",
            "Last ?buffer_dir result.", "")
        self._buffer1_size = Sensor.integer("buffer1_size",
            "Last ?buffer1_size result.", "")
        self._buffer2_size = Sensor.integer("buffer2_size",
            "Last ?buffer_size result.", "")

        self.add_sensor(self._buffer_dir)

        self._buffer_dir.set_value("/var/kat/data/tape_buffer1")

    def __init__(self, server_host, server_port):
        DeviceServer.__init__(self, server_host, server_port)
        # self.ta = tape_archive.TapeArchive()
        self.set_concurrency_options(False, False)
        signal.signal(signal.SIGINT, signal_handler)

    @request()
    @return_reply(Str())
    def request_swap_buffer(self, req):
        """Set the buffer_dir sensor"""
        self.buffer_index = (self.buffer_index + 1) % 2
        self._buffer_dir.set_value(buffer_dirs[self.buffer_index])
        return ("ok", "buffer_dir_set_to_%s"%buffer_dirs[self.buffer_index])

    @request(Str())
    @return_reply(Str())
    def request_set_buffer_dir(self, req, buffer_dir):
        """Set the buffer_dir sensor"""
        self._buffer_dir.set_value(buffer_dir)
        return ("ok", "buffer_dir_set_to_%s"%buffer_dir)

    @request(Int(), Int())
    @return_reply(Str())
    def request_set_buffer_size(self, req, bufnum, size):
        """Set the buffer_dir sensor"""
        if bufnum == 1:
            self._buffer1_size.set_value(size)
        elif bufnum == 2:
            self.buffer2_size.set_value(size)
        else:
            return ("fail", "no buffer %d"%bufnum)
        return ("ok", "buffer_dir_set_to_%s"%buffer_dir)

    @request(Str())
    @return_reply(Str())
    def request_print_state(self, req, table):
        """Print the state of the tape archive.
        Display all with argument 'ALL'.
        Display only tape states with 'TAPE'.
        Display only drive states with 'DRIVE'.
        Display only slots states with 'SLOT'.
        Display only magazine states with 'MAGAZINE'
        """
        ta = tape_archive.TapeArchive()
        # ta.get_state()
        ret = ""
        if table == "ALL":
            ret = ta.print_state()
        elif table != "TAPE" and table != "DRIVE" and table != "SLOT" and table != "MAGAZINE":
            ta.close()
            return ('fail',"Bad argument %s.\n The options are ALL, TAPE, DRIVE, SLOT and MAGAZINE")
        else:
            ret = ta.print_state(table=table)
        ta.close()
        for line in ret.split("\n"):
            req.inform(line.replace(" ","_"))
        return ('ok', "print-state_COMPLETE")

    @request()
    @return_reply(Str())
    def request_create_tables (self, req):
        """Create tables"""
        ta = tape_archive.TapeArchive()
        ta.create_tables()
        ta.close()
        return ('ok', 'Tables created')

    @request()
    @return_reply(Str())
    def request_get_state(self, req):
        """Get state of tape"""
        ta = tape_archive.TapeArchive()
        ta.get_state()
        ta.close()
        return ('ok', 'Retrieved state, tables updated')

    @request(Str(), Str(), Str())
    @return_reply(Str())
    def request_load_tape(self, req,  driveid=None, tapeid = None, slotid = None):
        """Load a tape a drive.
        The drive can be specified with driveid.
        The tape to load can be specified by tapeid or the slotid.
        If there is already a tape in the provided drive, it will be unloaded.
        In the case none or some of these values are not provided, they will be chosen for the user.
        Returns a tuple of the drive which has been loaded"""
        if driveid == "-":
            driveid = None
        else:
            driveid = int(driveid)
        if tapeid == "-":
            tapeid = None
        if slotid == "-":
            slotid = None
        else:
            slotid=int(slotid)
        ta = tape_archive.TapeArchive()
        print (driveid, tapeid, slotid)
        try:
            slot, drive = ta.load_tape(driveid, tapeid, slotid)
        except:
            ta.close()
            raise
            # return ("fail", "no_free_drives")
        ta.close()

        return ("ok", "drive_%d_loaded_from_slot_%d"%(slot, drive))


    @request(Str())
    @return_reply(Str())
    def request_get_location_of_tape(self, req, tape):
        """Get the slot and drive that a tape is loaded in."""
        ta = tape_archive.TapeArchive()
        res = ta.get_location_of_tape(tape)
        ta.close()
        return ("ok", "%s is in slot %d and drive %d"%(tape, res[0], res[1] or -1))

    @request()
    @return_reply(Str())
    def request_get_free_drives(self, req):
        """Get free drives.
        returns a list of free drives, each drive will have a tuple (id, state)"""
        ta = tape_archive.TapeArchive()
        ret = ta.get_free_drives()
        for line in ret:
            req.inform(("drive %d"%line[0]).replace(" ", "_"))
        print ret
        if len(ret) < 1:
            req.inform("No_free_drives")
        ta.close()
        return ('ok', "get-free-drives_COMPLETE")

    @request()
    @return_reply(Str())
    def request_get_empty_tapes(self, req):
        """Get list of empty tapes and the slots they belong to.
        Returns a list of tuples (tapeid,slotid)"""
        ta = tape_archive.TapeArchive()
        ret = ta.get_empty_tapes()
        for line in ret:
            req.inform(("tape %s"%line[0]).replace(" ", "_"))
        if len(ret) < 1:
            req.inform("No_free_tapes")
        ta.close()
        return ('ok', "get-empty-tapes_COMPLETE")

    @request(Int(), Int())
    @return_reply(Str())
    def request_load (self, req, slot, drive):
        """Load tape from slot to drive"""
        ta = tape_archive.TapeArchive()
        ta.load(slot,drive)
        ta.close()
        return ('ok', "drive %d loaded with tape from slot %d"%(drive, slot))

    @request(Int())
    @return_reply(Str())
    def request_unload(self, req, drive):
        """Remove tape from drive"""
        ta = tape_archive.TapeArchive()
        try:
            ta.unload(drive)
        except:
            return ('fail', 'No_tape_in_drive_%d'%drive)
        ta.close()
        return ('ok', "drive_%d_unloaded"%drive)

    @request()
    @return_reply(Str())
    def request_get_free_slots(self, req):
        """Get free slots"""
        ta = tape_archive.TapeArchive()
        ret = ta.get_free_slots()
        for line in ret:
            req.inform("slot %d"%line[0])
        ta.close()
        return ('ok', "get-free-slots COMPLETE")

    @request(Str(),Int())
    @return_reply(Str())
    def request_write_buffer_to_tape(self, req, buffer_dir, drive):
        """Write the buffer to a empty tape"""
        ta = tape_archive.TapeArchive()
        try:
            tape = ta.write_buffer_to_tape(buffer_dir, drive)
        except Exception, e:
            ta.close()
            return ('fail', str(e).replace(' ', '_'))
            print tape
        ta.close()
        return('ok', 'Wrote  to tape')



    @request(Str(), Int())
    @return_reply(Str())
    def request_tar_folder_to_tape(self, req, folder, drive):
        """Tar folder to tap"""
        ta = tape_archive.TapeArchive()
        ta.tar_folder_to_tape(folder, drive)
        ta.close()
        return ('ok', "Folder %s tarred to drive %d", folder, drive)

    @request(Int())
    @return_reply(Str())
    def request_rewind_drive(self, req, drive):
        """Rewind drive"""
        ta = rewind_drive(drive)
        ta.tar_folder_to_tape(folder, drive)
        ta.close()
        return ('ok', "Tape in drive %d rewound", drive)

    @request(Int())
    @return_reply(Str())
    def request_get_file_list (self, req, drive):
        """Take in a drive number and return the files stored on each of the tars on the file.
        Returns a list of strings, string contains all the files in the corresponding tar"""
        ta = tape_archive.TapeArchive()
        ret = ta.get_file_list(drive)
        for line in ret.split("\n"):
            req.inform(line)
        ta.close()
        return ('ok', "get-file-list COMPLETE")

    @request(Int(), Str(), Str(), Int())
    @return_reply(Str())
    def request_read_file(self, req, drive, filenames, write_location, tar_num = 0):
        """File to location"""
        ta = rewind_drive(drive)
        ta.read_file(drive, filenames, write_location, tar_num)
        ta.close()
        return ('ok', "Folder %s written to %s from drive %d", filenames, write_location, drive)

    @request(Int())
    @return_reply(Str())
    def request_end_of_last_tar (self, req, drive):
        """To end of tape in drive"""
        ta = rewind_drive(drive)
        ta.end_of_last_tar(drive)
        ta.close()
        return ('ok', "At end of last tar on drive %d", drive)

    @request()
    @return_reply()
    def request_close(self, req):
        """DO the thing"""
        # import pdb
        # pdb.set_trace()
        self.stop()
        return('ok',)

    def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)

if __name__ == "__main__":
    server = tape_katcp_server(server_host, server_port)
    # server.set_ioloop()
    server.start()
    server.join()
