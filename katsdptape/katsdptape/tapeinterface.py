"""Interface for the tape interface"""
import logging
import os
import re
import MySQLdb as sql
import subprocess

#from katcp import DeviceServer, Sensor, ProtocolFlags, AsyncReply
#from katcp.kattypes import (Str, Float, Timestamp, Discrete, Int, request, return_reply)
from config import config as cnf
from katcp import DeviceServer, Sensor
from katcp.kattypes import (Str, Int, request, return_reply)

storage_element_regex = re.compile(" Storage Element \d{1,3}.+\n")
data_transfer_regex = re.compile("Data Transfer Element \d{1}:.+\n")
os_drives_regex = re.compile('\d{4}L6')

logger = logging.getLogger("katsdptape.katsdptapeinterface")

class TapeMachineInterface(object):
    """docstring for TapeMachineInterface"""
    def __init__(self, dbLocation = cnf["DB_location"], buffer_dir = cnf["buffer_dir"], loglevel = logging.DEBUG):
        super(TapeMachineInterface, self).__init__()

        self.buffer_dirs=["%s/buffer1"%buffer_dir, "%s/buffer2"%buffer_dir]
        self.buffer_index = 0

        logger.info('Initialising TapeMachineInterface with state database at %s'%dbLocation)
        self.db = sql.connect('localhost', 'kat', 'kat', 'tape_db')
        self.cur = self.db.cursor()
        self.create_tables()
        self.get_state()

    def swap_buffer(self):
        self.buffer_index = (self.buffer_index + 1) % 2

    def create_tables (self):
        logger.info('Creating magazine table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS magazine (
                id INTEGER PRIMARY KEY,
                state TEXT)''')
        logger.info('Creating slot table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS slot (
                id INTEGER PRIMARY KEY,
                type TEXT,
                magazine_id INTEGER,
                FOREIGN KEY (magazine_id) REFERENCES magazine(id))''')
        logger.info('Creating tape table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS tape (
                id VARCHAR(10) PRIMARY KEY,
                bytes_written INTEGER,
                size INTEGER,
                slot_id INTEGER,
                FOREIGN KEY (slot_id) REFERENCES slot(id))''')
        logger.info('Creating drive table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS drive (
                id INTEGER PRIMARY KEY,
                num_writes INTEGER,
                num_reads INTEGER,
                num_cleans INTEGER,
                magazine_id INTEGER,
                state TEXT,
                attached INTEGER,
                tape_id VARCHAR(10),
                FOREIGN KEY (magazine_id) REFERENCES magazine(id),
                FOREIGN KEY (tape_id) REFERENCES tape(id))''')
        self.db.commit()
        logger.info('Committed changes')

    def close(self):
        self.db.close()

    def get_state(self):
        """Queries the tape machine to find the state of the tapes, slots and drives.
        Updates the DB accordingly. This will update the location of tapes"""
        logger.info('Querying tape machine with command [sudo mtx-f/dev/%s status]'%cnf["controller"])
        cmd = subprocess.Popen(["sudo","mtx", "-f", "/dev/%s"%cnf["controller"], "status"], stdout=subprocess.PIPE)
        cmd.wait()
        out, err = cmd.communicate()
        logger.debug('Response %s\n Err %s'%(out, str(err)))
        storage_elements = storage_element_regex.findall(out)
        num_regex = re.compile ("\d{1,3}")

        count = 0

        logger.debug('Storage elements returned by regex of %s :\n%s'%(storage_element_regex.pattern, "\n".join(storage_elements)))
        logger.info("Adding tapes, slots and magazines")

        for s_e in storage_elements:
            slot_id = int(num_regex.findall(s_e)[0])
            # print slot_id
            t = "MAGAZINE"
            if "IMPORT" in s_e:
                t = "MAIL"
            if (slot_id-1) % 30 == 0:
                self.logger.debug('Adding magazine %d'%(slot_id-1/30))
                self.cur.execute("""
                        INSERT IGNORE INTO magazine (id, state)
                        VALUES (%s,%s)""", ((slot_id-1)/30, "LOCKED"))
            if "Full" in s_e:
                tape_id = s_e[-7:-1]
                if "Full" in tape_id:
                    tape_id = "NO LABEL - %03d"%count
                    count+=1
                logger.debug('Adding tape %s in slot %d in magazine %d'%(tape_id, slot_id, slot_id/30))
                self.cur.execute("""
                    INSERT IGNORE INTO slot (id, type, magazine_id)
                    VALUES (%s,%s,%s)""", (slot_id, t,(slot_id-1)/30))
                self.cur.execute("""
                    INSERT IGNORE INTO tape(id, slot_id, bytes_written, size)
                    VALUES (%s,%s,%s,%s)""", (tape_id, slot_id, 0, cnf["tape_size_limit"]))
            else:
                logger.debug('Adding empty slot %d in magazine %d'%(slot_id, (slot_id-1)/30))
                self.cur.execute("""
                    INSERT IGNORE INTO slot (id, type, magazine_id)
                    VALUES (%s,%s,%s)""", (slot_id, t,(slot_id-1)/30))

            if (slot_id-1) % 30 == 0:
                logger.debug('Adding magazine %d'%(slot_id-1/30))
                self.cur.execute("""
                        INSERT OR REPLACE INTO magazine (id, state)
                        VALUES (%s,%s)""", ((slot_id-1)/30, "LOCKED"))

        data_transfer = data_transfer_regex.findall(out)

        free_slots = self.get_free_slots()
        print free_slots

        cmd = subprocess.Popen(["lsscsi"], stdout=subprocess.PIPE)
        cmd.wait()
        out, err = cmd.communicate()

        logger.info("Adding drives")

        count = 0

        for d_t in data_transfer:
            attached = 0
            drive_id = int(num_regex.findall(d_t)[0])
            if any(s in out for s in cnf['drive%d'%drive_id]):
                attached = 1
            if "Full" in d_t:
                logger.debug('Adding tape %s in drive %d and slot %d in magazine %d'%(d_t[-7:-1], drive_id, slot_id, drive_id/2))
                self.cur.execute("""
                    INSERT IGNORE INTO tape(id, bytes_written, size, slot_id)
                    VALUES (%s, %s, %s, %s)""", (d_t[-7:-1], 0, cnf["tape_size_limit"], free_slots[count][0]))
                count +=1
                self.cur.execute("""
                    INSERT INTO drive (id, state, magazine_id, num_writes, num_reads, num_cleans, tape_id, attached)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE tape_id=%s, attached=%s, state=%s""", (
                                    drive_id, "IDLE", drive_id/2, 0, 0, 0, d_t[-7:-1], attached,
                                    d_t[-7:-1], attached, "IDLE"))
            else:
                self.cur.execute("""
                    INSERT INTO drive (id, state, magazine_id, num_writes, num_reads, num_cleans, attached)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE tape_id=NULL, attached=%s, state=%s""", (
                                    drive_id, "IDLE", drive_id/2, 0, 0, 0, attached,
                                    attached, "IDLE"))

        self.db.commit()
        logger.info("Committing DB")
        logger.debug ("DB state :\n%s"%self.print_state())

    def print_state(self, table = None):
        """Get the state of the TapeMachineInterface from the DB.
        Can choose which table to check by using the table argument.
        The options are "TAPE", "SLOT", "DRIVE", "MAGAZINE". If no table is selected, all tables states are returned.
        Returns a formatted string of the state."""
        # self.get_state()
        logger.info("Getting state for table = %s"%str(table))
        ret = ""

        if table == None or table == "TAPE":
            ret += "#############\n# TAPES     #\n#############\n"
            self.cur.execute("""SELECT * FROM tape""")
            all_rows = self.cur.fetchall()
            for row in all_rows:
                ret+=", ".join(str(item) for item in row)+"\n"

        if table == None or table == "SLOT":
            ret+= "#############\n# SLOTS     #\n#############\n"
            self.cur.execute("""SELECT * FROM slot""")
            all_rows = self.cur.fetchall()
            for row in all_rows:
                ret+=", ".join(str(item) for item in row)+"\n"

        if table == None or table == "DRIVE":
            ret+= "#############\n# DRIVES    #\n#############\n"
            self.cur.execute("""SELECT * FROM drive""")
            all_rows = self.cur.fetchall()
            for row in all_rows:
                ret+=", ".join(str(item) for item in row)+"\n"

        if table == None or table == "MAGAZINE":
            ret+= "#############\n# MAGAZINES #\n#############\n"
            self.cur.execute("""SELECT * FROM magazine""")
            all_rows = self.cur.fetchall()
            for row in all_rows:
                ret+=", ".join(str(item) for item in row)+"\n"
        return ret


    def load_tape(self,  driveid=None, tapeid = None, slotid = None):
        """Load a tape a drive.
        The drive can be specified with driveid.
        The tape to load can be specified by tapeid or the slotid.
        If there is already a tape in the provided drive, it will be unloaded.
        In the case none or some of these values are not provided, they will be chosen for the user.
        Returns a tuple of the drive which has been loaded"""
        self.get_state()

        drive = [None,None]
        slot = None

        logger.info("Load_tape with driveid = %s, tapeid = %s, slotid=%s"%(str(driveid or "na"), str(tapeid or "na"), str(slotid or "na")))

        if driveid == None:
            logger.info("No drive provided, choosing a free drive")
            free_drives = self.get_free_drives()
            if len(free_drives) == 0:
                logger.error("No Free Drives")
                raise Exception("No Free Drives")
            else:
                drive=free_drives[0]
                driveid = drive[0]
                logger.info("Using drive %d"%driveid)

        if drive[1] == 'FULL':
            logger.info("Drive %d is full"%driveid)
            self.unload(driveid)

        if (tapeid != None):
            tape = None
            try:
                slot = self.get_location_of_tape(tapeid)
                slotid=slot[0]
            except:
                logger.error("%s is not a valid tape id"%tapeid, exc_info=True)
                raise
            if tape[1] != None:
                loc = self.get_location_of_tape(tapeid)
                print "%s is already loaded in drive"%(tapeid, loc[1])
                return loc[1]

        if (tapeid == None and slotid == None):
            logger.info("No tape or slot provided")
            tapes = self.get_empty_tapes()
            if len(tapes) == 0:
                logger.error("No empty tapes")
                raise Exception("No empty tapes")
            else:
                logger.info("Chose tape %s from slot %d"%(tapes[0][0], tapes[0][1]))
                slotid = tapes[0][1]
        
        self.load(slotid, driveid)

        return slotid, driveid

    def get_location_of_tape(self, tape):
        """Get the slot and drive that a tape is loaded in."""
        self.get_state()

        logger.info("Getting location for tape %s"%tape)
        self.cur.execute(
            """SELECT tape.slot_id, drive.id
            FROM tape LEFT OUTER JOIN drive ON drive.tape_id = tape.id
            WHERE tape.id = \'%s\'"""%(
                tape,))
        res = self.cur.fetchone ()
        self.db.commit()
        return res

    def get_drive (self, drive):
        """Get drive info"""
        self.get_state()

        logger.info("Getting drive %d info")
        self.cur.execute(
            """SELECT * FROM drive LEFT OUTER JOIN tape ON drive.tape_id = tape.id
            WHERE drive.id = %d"""%(
                drive,))
        names = list(map(lambda x: x[0], self.cur.description))
        # print names
        res = self.cur.fetchone()
        return [names,res]

    def write_buffer_to_tape(self, buffer_dir, drive):
        self.get_state()

        res = self.get_drive(drive)
        tape = None
        if res[1][res[0].index("attached")] == 0:
            raise Exception ("Drive not attached for writing")
        if res[1][res[0].index("tape_id")] == None:
            tape = self.load_empty_tape(drive)
        elif res[1][res[0].index("bytes_written")] > 0:
            self.unload(drive)
            tape = self.load_empty_tape(drive)
        self.rewind_drive(drive)
        self.tar_folder_to_tape(buffer_dir, drive)
        self.unload(drive)

        return tape

    def get_free_drives(self, magazine = None):
        """Get free drives.
        returns a list of free drives, each drive will have a tuple (id, state)"""
        self.get_state()

        logger.info("Getting free drives")
        if magazine == None:
            self.cur.execute(
                """SELECT drive.id, drive.state 
                FROM drive LEFT OUTER JOIN tape ON drive.tape_id = tape.id 
                WHERE drive.state = 'EMPTY' OR tape.bytes_written > %d 
                ORDER BY drive.state"""%(
                    cnf["tape_size_limit"],))
        else:
            self.cur.execute("""SELECT drive.id, drive.state 
                FROM drive LEFT OUTER JOIN tape ON drive.tape_id = tape.id 
                WHERE (drive.state = 'EMPTY' OR tape.bytes_written > %d) AND drive.magazine_id = %d 
                ORDER BY drive.state"""%(
                    cnf["tape_size_limit"], magazine))
        self.db.commit()

        res = self.cur.fetchall()
        return res

    """Get list of empty tapes and the slots they belong to.
    Returns a list of tuples (tapeid,slotid)"""
    def get_empty_tapes(self, magazine = None):
        self.get_state()

        logger.info("Getting empty tapes")
        if magazine == None:
            self.cur.execute(
                """SELECT tape.id, slot.id
                FROM tape JOIN slot ON tape.slot_id = slot.id 
                WHERE tape.bytes_written < %d 
                ORDER BY slot.id"""%(
                    cnf["tape_size_limit"],))
        else:
            self.cur.execute("""SELECT tape.id, slot.id
                FROM tape JOIN slot ON tape.slot_id = slot.id  
                WHERE tape.bytes_written < %d  AND slot.magazine_id = %d 
                ORDER BY drive.state"""%(
                    cnf["tape_size_limit"], magazine))
        self.db.commit()

        res = self.cur.fetchall()
        return res

    def load_empty_tape (self, drive):
        self.get_state()

        # logger.info("Loading empty tape to drive %d"%drive)
        res = self.get_empty_tapes()
        logger.info ("%d empty tapes"%len(res))
        if len(res) < 1:
            raise Exception("No empty tapes")
        else:
            logger.info("Loading empty tape %s to drive %d from slot %d"%(res[0][0], drive, res[0][1]))
            self.load(res[0][1], drive)

        return [['tape_id','drive_id'],res[0]]


    """Load tape from slot to drive"""
    def load (self, slot, drive):
        self.get_state()

        logger.info("Loading tape from slot %d to drive %d"%(slot, drive))
        self.cur.execute(
            """SELECT tape.id
            FROM tape
            WHERE slot_id = %d"""%(
                slot))
        res = self.cur.fetchone ()
        logger.debug("Updating drive table for loading")
        self.cur.execute(
            """UPDATE drive
            SET state = 'LOADING', tape_id = '%s'
            WHERE id = %d"""%(
                res[0], drive))
        self.db.commit()
        logger.debug("Running command sudo mtx -f /dev/%s load %d %d"%(cnf["controller"], slot, drive))
        cmd=subprocess.Popen(["sudo","mtx","-f","/dev/%s"%cnf["controller"],"load", str(slot), str(drive)], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

        logger.debug("Updating drive table to IDLE")
        self.cur.execute(
            """UPDATE drive
            set state='IDLE'
            where id=%d"""%(
                drive))
        self.db.commit()
        logger.info("Tape loaded, DB updated")

    """Remove tape from drive"""
    def unload(self, drive):
        self.get_state()

        logger.info("Unloading drive %d"%drive)
        self.cur.execute(
            """SELECT tape.slot_id 
            FROM drive INNER JOIN tape ON drive.tape_id == tape.id 
            WHERE drive.id = %d"""%(
                drive,))
        res = self.cur.fetchone ()
        self.cur.execute (
            """UPDATE drive
            SET state = 'UNLOADING'
            WHERE id = %d"""%(
                drive,))
        

        if (len(res) < 1):
            raise Exception("No tape in drive")

        self.db.commit()

        logger.debug("Running command sudo mtx -f /dev/%s unload %d %d"%(cnf["controller"], res[0], drive))
        
        cmd=subprocess.Popen(["sudo","mtx","-f","/dev/%s"%cnf["controller"],"unload", str(res[0]), str(drive)], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

        self.cur.execute (
            """UPDATE drive
            SET state = 'EMPTY', tape_id = NULL
            WHERE id = %d"""%(
                drive,))

        self.db.commit()
        logger.info ("Drive unloaded, DB updated")

    """Get free slots"""
    def get_free_slots(self):
        # self.get_state()

        logger.info("Getting free slots")
        self.cur.execute(
            """SELECT slot.id
            FROM slot LEFT OUTER JOIN tape ON slot.id = tape.slot_id
            WHERE tape.slot_id IS NULL""")
        self.db.commit()
        return self.cur.fetchall()

    """Tar folder to tape in drive.
    Returns the id of the tape in that drive"""
    def tar_folder_to_tape(self, folder, drive):
        self.get_state()

        logger.info("Taring folder %s to tape in drive %d"%(folder, drive))
        self.cur.execute(
            """SELECT attached, tape_id
            FROM drive
            WHERE id = %s"""%(
                drive,))

        res = self.cur.fetchone()
        if res[0] == 1 and res[1] != None:
            size =int(subprocess.check_output(["du","-s", folder]).split()[0])
            self.cur.execute(
                """UPDATE drive
                SET state = 'WRITING', num_writes = num_writes + 1
                WHERE id = %d"""%(
                    drive,))
            self.db.commit()

            path = folder.split("/")
            print "/".join(path[:-1])

            os.chdir("/".join(path[:-1]))
            logger.debug("Taring folder %s with size %d bytes to tape %s in drive %d with :\n tar cvf /dev/%s %s"%(
                folder, size, res[1], drive, cnf["drive%s"%drive][0], path[-1]))

            cmd=subprocess.Popen(["sudo", "tar","cvf","/dev/%s"%cnf["drive%s"%drive][0], path[-1]], stdout=subprocess.PIPE)
            cmd.wait()
            comm=cmd.communicate()
            logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

            logger.info("Updating  DB")
            
            self.cur.execute(
                """UPDATE drive
                SET state = 'IDLE'
                WHERE id = %s"""%(
                    drive,))
            self.cur.execute(
                """UPDATE tape
                SET bytes_written = bytes_written + %d
                WHERE id = \'%s\'"""%(
                    size, res[1]))
            self.db.commit()
            logger.info("Committed changes to DB")
        else:
            print "ERROR while writing to drive. Attached = %d, tape = %s"%res

    def rewind_drive(self, drive):
        self.get_state()

        """Rewind drive"""
        logger.info("Rewinding tape in drive %d"%drive)
        self.cur.execute(
                """UPDATE drive
                SET state = 'REWINDING'
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()
        logger.debug("Rewinding with command mt -f /dev/%s rewind"%cnf["drive%s"%drive][0] )
        cmd=subprocess.Popen(["sudo", "mt","-f", "/dev/%s"%cnf["drive%s"%drive][0], "rewind"], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))
        self.cur.execute(
                """UPDATE drive
                SET state = 'IDLE'
                WHERE id = %s"""%(
                    drive,))
        self.db.commit()
        logger.info("Committed changes to DB")

    def get_file_list (self, drive):
        """Take in a drive number and return the files stored on each of the tars on the file.
        Returns a list of strings, string contains all the files in the corresponding tar"""
        self.get_state()

        self.rewind_drive(drive)
        self.cur.execute(
                """UPDATE drive
                SET state = 'READING'
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()
        cmd=subprocess.Popen(["tar","-tf","/dev/n%s"%cnf["drive%s"%drive][0]], stdout=subprocess.PIPE)
        cmd.wait()
        ret = []
        count = 0
        out = cmd.communicate()[0]

        while cmd.returncode == 0:
            print "----------------"
            print out
            ret.append(out)
            cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
            cmd.wait()
            cmd=subprocess.Popen(["sudo", "tar","-tf","/dev/n%s"%cnf["drive%s"%drive][0]], stdout=subprocess.PIPE)
            cmd.wait()
            out = cmd.communicate()[0]
            count += 1

        cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "bsfm", "1"], stdout=subprocess.PIPE)
        cmd.wait()

        self.cur.execute(
                """UPDATE drive
                SET state = 'IDLE', num_reads = num_reads + 1
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()

        return ret

    def read_file(self, drive, filenames, write_location, tar_num = 0):
        self.get_state()

        self.rewind_drive(drive)

        self.cur.execute(
                """UPDATE drive
                SET state = 'READING'
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()

        os.chdir(write_location)

        for i in range(tar_num):
            print "FORWARD"
            cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
            cmd.wait()

        cmd=subprocess.Popen(["sudo", "tar","-xvf","/dev/n%s"%cnf["drive%s"%drive][0], filenames], stdout=subprocess.PIPE)
        cmd.wait()
        print cmd.communicate()

        self.cur.execute(
                """UPDATE drive
                SET state = 'IDLE', num_reads = num_reads + 1
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()


    def end_of_last_tar (self, drive):
        self.get_state()

        cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
        cmd.wait()
        while cmd.returncode == 0:
            cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
            cmd.wait()


class TapeDeviceServer(DeviceServer):

    VERSION_INFO = ("tape_katcp_interface", 1, 0)
    BUILD_INFO = ("tape_katcp_interface", 0, 1, "")

    def __init__(self, server_host, server_port, dbLocation = cnf["DB_location"], buffer_dir = cnf["buffer_dir"]):
        self.ta = TapeMachineInterface(dbLocation, buffer_dir)
        DeviceServer.__init__(self, server_host, server_port)
        self.set_concurrency_options(False, False)
        

    def setup_sensors(self):
        """Setup some server sensors."""
        self._buffer_dir = Sensor.string("buffer_dir",
            "Last ?buffer_dir result.", "")
        self._buffer1_size = Sensor.integer("buffer1_size",
            "Last ?buffer1_size result.", "")
        self._buffer2_size = Sensor.integer("buffer2_size",
            "Last ?buffer_size result.", "")

        self.add_sensor(self._buffer_dir)

        self._buffer_dir.set_value(self.ta.buffer_dirs[self.ta.buffer_index])

    @request()
    @return_reply(Str())
    def request_swap_buffer(self, req):
        """Set the buffer_dir sensor"""
        self.ta.swap_buffer()
        self._buffer_dir.set_value(self.ta.buffer_dirs[self.ta.buffer_index])
        return ("ok", "buffer_dir_set_to_%s"%self.ta.buffer_dirs[self.ta.buffer_index])

    # @request(Str()) Don't need this
    # @return_reply(Str())
    # def request_set_buffer_dir(self, req, buffer_dir):
    #     """Set the buffer_dir sensor"""
    #     self._buffer_dir.set_value(buffer_dir)
    #     return ("ok", "buffer_dir_set_to_%s" % buffer_dir)

    @request(Int(), Int())
    @return_reply(Str())
    def request_set_buffer_size(self, req, bufnum, size):
        """Set the buffer_dir sensor"""
        if bufnum == 1:
            self._buffer1_size.set_value(size)
        elif bufnum == 2:
            self.buffer2_size.set_value(size)
        else:
            return ("fail", "no buffer %d" % (bufnum,))
        return ("ok", "buffer_dir_set_to_%s" % (size,))

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
        self.ta.get_state()
        ret = ""
        if table == "ALL":
            ret = self.ta.print_state()
        elif table != "TAPE" and table != "DRIVE" and table != "SLOT" and table != "MAGAZINE":
            return ('fail',"Bad argument %s.\n The options are ALL, TAPE, DRIVE, SLOT and MAGAZINE")
        else:
            ret = self.ta.print_state(table=table)
        for line in ret.split("\n"):
            req.inform(line.replace(" ","_"))
        return ('ok', "print-state_COMPLETE")

    @request()
    @return_reply(Str())
    def request_create_tables (self, req):
        """Create tables"""
        self.ta.create_tables()
        return ('ok', 'Tables created')

    @request()
    @return_reply(Str())
    def request_get_state(self, req):
        """Get state of tape"""
        self.ta.get_state()
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
        print (driveid, tapeid, slotid)
        try:
            slot, drive = self.ta.load_tape(driveid, tapeid, slotid)
        except:
            raise
            # return ("fail", "no_free_drives")

        return ("ok", "drive_%d_loaded_from_slot_%d"%(slot, drive))


    @request(Str())
    @return_reply(Str())
    def request_get_location_of_tape(self, req, tape):
        """Get the slot and drive that a tape is loaded in."""
        res = self.ta.get_location_of_tape(tape)
        return ("ok", "%s is in slot %d and drive %d"%(tape, res[0], res[1] or -1))

    @request()
    @return_reply(Str())
    def request_get_free_drives(self, req):
        """Get free drives.
        returns a list of free drives, each drive will have a tuple (id, state)"""
        ret = self.ta.get_free_drives()
        for line in ret:
            req.inform(("drive %d"%line[0]).replace(" ", "_"))
        print ret
        if len(ret) < 1:
            req.inform("No_free_drives")
        return ('ok', "get-free-drives_COMPLETE")

    @request()
    @return_reply(Str())
    def request_get_empty_tapes(self, req):
        """Get list of empty tapes and the slots they belong to.
        Returns a list of tuples (tapeid,slotid)"""
        ret = self.ta.get_empty_tapes()
        for line in ret:
            req.inform(("tape %s"%line[0]).replace(" ", "_"))
        if len(ret) < 1:
            req.inform("No_free_tapes")
        return ('ok', "get-empty-tapes_COMPLETE")

    @request(Int(), Int())
    @return_reply(Str())
    def request_load (self, req, slot, drive):
        """Load tape from slot to drive"""
        self.ta.load(slot,drive)
        return ('ok', "drive %d loaded with tape from slot %d"%(drive, slot))

    @request(Int())
    @return_reply(Str())
    def request_unload(self, req, drive):
        """Remove tape from drive"""
        try:
            self.ta.unload(drive)
        except:
            return ('fail', 'No_tape_in_drive_%d'%drive)
        return ('ok', "drive_%d_unloaded"%drive)

    @request()
    @return_reply(Str())
    def request_get_free_slots(self, req):
        """Get free slots"""
        ret = self.ta.get_free_slots()
        for line in ret:
            req.inform("slot %d"%line[0])
        return ('ok', "get-free-slots COMPLETE")

    @request(Str(),Int())
    @return_reply(Str())
    def request_write_buffer_to_tape(self, req, buffer_dir, drive):
        """Write the buffer to a empty tape"""
        try:
            tape = self.ta.write_buffer_to_tape(buffer_dir, drive)
        except Exception, e:
            return ('fail', str(e).replace(' ', '_'))
        return('ok', 'Wrote  to tape')

    @request(Str(), Int())
    @return_reply(Str())
    def request_tar_folder_to_tape(self, req, folder, drive):
        """Tar folder to tap"""
        self.ta.tar_folder_to_tape(folder, drive)
        return ('ok', "Folder %s tarred to drive %d", folder, drive)

    @request(Int())
    @return_reply(Str())
    def request_rewind_drive(self, req, drive):
        """Rewind drive"""
        self.ta.rewind_drive(drive)
        return ('ok', "Tape in drive %d rewound", drive)

    @request(Int())
    @return_reply(Str())
    def request_get_file_list (self, req, drive):
        """Take in a drive number and return the files stored on each of the tars on the file.
        Returns a list of strings, string contains all the files in the corresponding tar"""
        ret = self.ta.get_file_list(drive)
        for line in ret.split("\n"):
            req.inform(line)
        return ('ok', "get-file-list COMPLETE")

    @request(Int(), Str(), Str(), Int())
    @return_reply(Str())
    def request_read_file(self, req, drive, filenames, write_location, tar_num = 0):
        """File to location"""
        self.ta.rewind_drive(drive)
        self.ta.read_file(drive, filenames, write_location, tar_num)
        return ('ok', "Folder %s written to %s from drive %d", filenames, write_location, drive)

    @request(Int())
    @return_reply(Str())
    def request_end_of_last_tar (self, req, drive):
        """To end of tape in drive"""
        self.ta.rewind_drive(drive)
        self.ta.end_of_last_tar(drive)
        return ('ok', "At end of last tar on drive %d", drive)

    def handle_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Vis Store Controller interrupted.")
