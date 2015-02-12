from katcp import DeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Float, Timestamp, Discrete, Int, request, return_reply)

import threading
import time
import random

import tape_archive

server_host = "192.168.6.66"
server_port = 5000

class tape_katcp_server(DeviceServer):

    VERSION_INFO = ("tape_katcp_interface", 1, 0)
    BUILD_INFO = ("tape_katcp_interface", 0, 1, "")

    # Optionally set the KATCP protocol version and features. Defaults to
    # the latest implemented version of KATCP, with all supported optional
    # features
    # PROTOCOL_INFO = ProtocolFlags(5, 0, set([
    #     ProtocolFlags.MULTI_CLIENT,
    #     ProtocolFlags.MESSAGE_IDS,
    # ]))
    

    def setup_sensors(self):
        """Setup some server sensors."""
        self._add_result = Sensor.float("add.result",
            "Last ?add result.", "", [-10000, 10000])

        self._time_result = Sensor.timestamp("time.result",
            "Last ?time result.", "")

        self._eval_result = Sensor.string("eval.result",
            "Last ?eval result.", "")

        

        self.add_sensor(self._add_result)
        self.add_sensor(self._time_result)
        self.add_sensor(self._eval_result)

    def __init__(self, server_host, server_port):
        DeviceServer.__init__(self, server_host, server_port)
        # self.ta = tape_archive.tape_archive()
        self.set_concurrency_options(False, False)

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
        ta = tape_archive.tape_archive()
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
            req.inform(line)
        return ('ok', "print-state COMPLETE")

    @request()
    @return_reply(Str())
    def request_create_tables (self):
        """Create tables"""
        ta = tape_archive.tape_archive()
        ta.create_tables()
        ta.close()
        return ('ok', 'Tables created')

    @request()
    @return_reply(Str())
    def request_get_state(self):
        """Get state of tape"""
        ta = tape_archive.tape_archive()
        ta.get_state()
        ta.close()
        return ('ok', 'Retrieved state, tables updated')

    @request(Str(), Str(), Str())
    @return_reply(Str())
    def request_load_tape(self,  driveid=None, tapeid = None, slotid = None):
        """Load a tape a drive.
        The drive can be specified with driveid.
        The tape to load can be specified by tapeid or the slotid.
        If there is already a tape in the provided drive, it will be unloaded.
        In the case none or some of these values are not provided, they will be chosen for the user.
        Returns a tuple of the drive which has been loaded"""
        if driveid == "-":
            driveid = None
        if tapeid == "-":
            tapeid = None
        if slotid == "-":
            slotid = None
        ta = tape_archive.tape_archive()
        slot, drive = ta.load_tape(driveid, tapeid, slotid)
        ta.close()

        return (ok, "drive %d loaded from slot %d"%(slot, drive))


    @request(Str())
    @return_reply(Str())
    def request_get_location_of_tape(self, tape):
        """Get the slot and drive that a tape is loaded in."""
        ta = tape_archive.tape_archive()
        res = ta.get_location_of_tape()
        ta.close()
        return ("ok", "%s is in slot %d"%(tape, res))

    @request(Str())
    @return_reply(Str())
    def request_get_free_drives(self, magazine = None):
        """Get free drives.
        returns a list of free drives, each drive will have a tuple (id, state)"""
        ta = tape_archive.tape_archive()
        if magazine == "-":
            magazine = None
        ret = ta.get_free_drives(int(magazine))
        for line in ret.split("\n"):
            req.inform(line)
        ta.close()
        return ('ok', "get-free-drives COMPLETE")

    @request(Str())
    @return_reply(Str())
    def request_get_empty_tapes(self, magazine = None):
        """Get list of empty tapes and the slots they belong to.
        Returns a list of tuples (tapeid,slotid)"""
        self.logger.info("Getting empty tapes")
        ta = tape_archive.tape_archive()
        if magazine == "-":
            magazine = None
        ret = ta.get_empty_tapes(int(magazine))
        for line in ret.split("\n"):
            req.inform(line)
        ta.close()
        return ('ok', "get-empty-tapes COMPLETE")

    @request(Int(), Int())
    @return_reply(Str())
    def request_load (self, slot, drive):
        """Load tape from slot to drive"""
        ta = tape_archive.tape_archive()
        ta.load(slot,drive)
        ta.close()
        return (ok, "drive %d loaded with tape from slot %d"%drive)

    @request(Int())
    @return_reply(Str())
    def request_unload(self, drive):
        """Remove tape from drive"""
        ta = tape_archive.tape_archive()
        ta.unload(drive)
        ta.close()
        return (ok, "drive %d unloaded"%drive)

    @request()
    @return_reply(Str())
    def request_get_free_slots(self):
        """Get free slots"""
        ta = tape_archive.tape_archive()
        ret = ta.get_free_slots()
        for line in ret.split("\n"):
            req.inform(line)
        ta.close()
        return ('ok', "get-free-slots COMPLETE")

    @request(Str(), Int())
    @return_reply(Str())
    def request_tar_folder_to_tape(self, folder, drive):
        """Tar folder to tap"""
        ta = tape_archive.tape_archive()
        ta.tar_folder_to_tape(folder, drive)
        ta.close()
        return ('ok', "Folder %s tarred to drive %d", folder, drive)

    @request(Int())
    @return_reply(Str())
    def request_rewind_drive(self, drive):
        """Rewind drive"""
        ta = rewind_drive(drive)
        ta.tar_folder_to_tape(folder, drive)
        ta.close()
        return ('ok', "Tape in drive %d rewound", drive)

    @request(Int())
    @return_reply(Str())
    def request_get_file_list (self, drive):
        """Take in a drive number and return the files stored on each of the tars on the file.
        Returns a list of strings, string contains all the files in the corresponding tar"""
        ta = tape_archive.tape_archive()
        ret = ta.get_file_list(drive)
        for line in ret.split("\n"):
            req.inform(line)
        ta.close()
        return ('ok', "get-file-list COMPLETE")

    @request(Int(), Str(), Str(), Int())
    @return_reply(Str())
    def request_read_file(self, drive, filenames, write_location, tar_num = 0):
        """File to location"""
        ta = rewind_drive(drive)
        ta.read_file(drive, filenames, write_location, tar_num)
        ta.close()
        return ('ok', "Folder %s written to %s from drive %d", filenames, write_location, drive)

    @request(Int())
    @return_reply(Str())
    def request_end_of_last_tar (self, drive):
        """To end of tape in drive"""
        ta = rewind_drive(drive)
        ta.end_of_last_tar(drive)
        ta.close()
        return ('ok', "At end of last tar on drive %d", drive)


if __name__ == "__main__":
    server = tape_katcp_server(server_host, server_port)
    # server.set_ioloop()
    server.start()
    server.join()