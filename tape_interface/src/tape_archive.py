"""Interface for the tape interface"""
import sqlite3 as sql
from config import config as cnf
import re
import subprocess
import os
import time
import logging

storage_element_regex = re.compile(" Storage Element \d{1,3}.+\n")
data_transfer_regex = re.compile("Data Transfer Element \d{1}:.+\n")
os_drives_regex = re.compile('\d{4}L6')


class tape_archive:

    def __init__(self, dbLocation = cnf["DB_location"]):
        # frmt = logging.Formatter()
        logging.basicConfig(format = '%(asctime)s - %(name)s - %(funcName)s -%(levelname)s - %(message)s', level = logging.DEBUG)
        
        self.logger = logging.getLogger("tape_archive")
        
        # self.logger.setFormatter(frmt)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info('Initialising tape_archive with state database at %s'%dbLocation)
        self.db = sql.connect(dbLocation)
        self.cur = self.db.cursor()
        self.create_tables()


    def create_tables (self):
        self.logger.info('Creating magazine table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS magazine (
                id INTEGER PRIMARY KEY,
                state TEXT)''')
        self.logger.info('Creating tape table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS tape (
                id TEXT PRIMARY KEY,
                use TEXT,
                bytes_written INTEGER,
                size INTEGER,
                slot_id INTEGER,
                FOREIGN KEY (slot_id) REFERENCES slot(id))''')
        self.logger.info('Creating drive table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS drive (
                id INTEGER,
                num_writes INTEGER,
                num_reads INTEGER,
                num_cleans INTEGER,
                magazine_id INTEGER,
                state TEXT,
                attached INTEGER,
                tape_id TEXT,
                FOREIGN KEY (magazine_id) REFERENCES magazine(id),
                FOREIGN KEY (tape_id) REFERENCES tape(id))''')
        self.logger.info('Creating slot table')
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS slot (
                id INTEGER PRIMARY KEY,
                type TEXT,
                magazine_id INTEGER,
                FOREIGN KEY (magazine_id) REFERENCES magazine(id))''')
        self.db.commit()
        self.logger.info('Committed changes')

    def close(self):
        self.db.close()

    """Queries the tape machine to find the state of the tapes, slots and drives.
    Updates the DB accordingly. This will only work if the DB is empty"""
    def get_state(self):
        self.logger.info('Querying tape machine with command [sudo mtx-f/dev/%s status]'%cnf["controller"])
        cmd = subprocess.Popen(["sudo","mtx", "-f", "/dev/%s"%cnf["controller"], "status"], stdout=subprocess.PIPE)
        cmd.wait()
        out, err = cmd.communicate()
        self.logger.debug('Response %s\n Err %s'%(out, str(err)))
        storage_elements = storage_element_regex.findall(out)
        num_regex = re.compile ("\d{1,3}")

        count = 0

        self.logger.debug('Storage elements returned by regex of %s :\n%s'%(storage_element_regex.pattern, "\n".join(storage_elements)))
        self.logger.info("Adding tapes, slots and magazines")

        for s_e in storage_elements:
            slot_id = int(num_regex.findall(s_e)[0])
            # print slot_id
            t = "MAGAZINE"
            if "IMPORT" in s_e:
                t = "MAIL"
                print t
            if "Full" in s_e:
                tape_id = s_e[-7:-1]
                if "Full" in tape_id:
                    tape_id = "NO LABEL - %03d"%count
                    count+=1
                self.logger.debug('Adding tape %s in slot %d in magazine %d'%(tape_id, slot_id, slot_id/30))
                self.cur.execute("""
                    INSERT INTO tape(id, slot_id, bytes_written, size)
                    VALUES (?,?,?,?)""", (tape_id, slot_id, 0, cnf["tape_size_limit"]))
                self.cur.execute("""
                    INSERT INTO slot (id, type, magazine_id)
                    VALUES (?,?,?)""", (slot_id, t,(slot_id-1)/30))
            else:
                self.logger.debug('Adding empty slot %d in magazine %d'%(slot_id, (slot_id-1)/30))
                self.cur.execute("""
                    INSERT INTO slot (id, type, magazine_id)
                    VALUES (?,?,?)""", (slot_id, t,(slot_id-1)/30))

            if (slot_id-1) % 30 == 0:
                self.logger.debug('Adding magazine %d'%(slot_id-1/30))
                self.cur.execute("""
                        INSERT INTO magazine (id, state)
                        VALUES (?,?)""", ((slot_id-1)/30, "LOCKED"))

        data_transfer = data_transfer_regex.findall(out)

        free_slots = self.get_free_slots()

        cmd = subprocess.Popen(["lsscsi"], stdout=subprocess.PIPE)
        cmd.wait()
        out, err = cmd.communicate()

        self.logger.info("Adding drives")

        for d_t in data_transfer:
            attached = 0
            drive_id = int(num_regex.findall(d_t)[0])
            if any(s in out for s in cnf['drive%d'%drive_id]):
                attached = 1
            if "Full" in d_t:
                self.logger.debug('Adding tape %s in drive %d and slot %d in magazine %d'%(d_t[-7:-1], drive_id, slot_id, drive_id/2))
                self.cur.execute("""
                    INSERT INTO tape(id, bytes_written, size, slot_id, size)
                    VALUES (?,?,?,?,?)""", (d_t[-7:-1], 0, cnf["tape_size_limit"], free_slots.pop()[0], cnf['tape_size_limit']))
                self.cur.execute("""
                    INSERT INTO drive (id, state, magazine_id, num_writes, num_reads, num_cleans, tape_id, attached)
                    VALUES (?,?,?,?,?,?,?,?)""", (drive_id, "IDLE", drive_id/2, 0, 0, 0, d_t[-7:-1], attached))
            else:
                self.cur.execute("""
                    INSERT INTO drive (id, state, magazine_id, num_writes, num_reads, num_cleans, attached)
                    VALUES (?,?,?,?,?,?,?)""", (drive_id, "EMPTY", drive_id/2, 0, 0, 0, attached))

        self.db.commit()
        self.logger.info("Committing DB")
        self.logger.debug ("DB state :\n%s"%self.print_state())


    """Get the state of the tape_archive from the DB.
    Can choose which table to check by using the table argument.
    The options are "TAPE", "SLOT", "DRIVE", "MAGAZINE". If no table is selected, all tables states are returned.
    Returns a formatted string of the state."""
    def print_state(self, table = None):
        self.logger.info("Getting state for table = %s"%str(table))
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


    """Load a tape a drive.
    The drive can be specified with driveid.
    The tape to load can be specified by tapeid or the slotid.
    If there is already a tape in the provided drive, it will be unloaded.
    In the case none or some of these values are not provided, they will be chosen for the user.
    Returns a tuple of the drive which has been loaded"""
    def load_tape(self,  driveid=None, tapeid = None, slotid = None):
        drive = [None,None]
        slot = None

        self.logger.info("Load_tape with driveid = %s, tapeid = %s, slotid=%s"%(str(driveid or "na"), str(tapeid or "na"), str(slotid or "na")))

        if driveid == None:
            self.logger.info("No drive provided, choosing a free drive")
            free_drives = self.get_free_drives()
            if len(free_drives) == 0:
                self.logger.error("No Free Drives")
                raise Exception("No Free Drives")
            else:
                drive=free_drives[0]
                driveid = drive[0]
                self.logger.info("Using drive %d"%driveid)

        if drive[1] == 'FULL':
            self.logger.info("Drive %d is full"%driveid)
            self.unload(driveid)

        if (tapeid != None):
            tape = None
            try:
                slot = self.get_location_of_tape(tapeid)
                slotid=slot[0]
            except:
                self.logger.error("%s is not a valid tape id"%tapeid, exc_info=True)
                raise
            if tape[1] != None:
                loc = self.get_location_of_tape(tapeid)
                print "%s is already loaded in drive"%(tapeid, loc[1])
                return loc[1]

        if (tapeid == None and slotid == None):
            self.logger.info("No tape or slot provided")
            tapes = self.get_empty_tapes()
            if len(tapes) == 0:
                self.logger.error("No empty tapes")
                raise Exception("No empty tapes")
            else:
                self.logger.info("Chose tape %s from slot %d"%(tapes[0][0], tapes[0][1]))
                slotid = tapes[0][1]
        
        self.load(slotid, driveid)

        return slotid, driveid

    """Get the slot and drive that a tape is loaded in."""
    def get_location_of_tape(self, tape):
        self.logger.info("Getting location for tape %s"%tape)
        self.cur.execute(
            """SELECT tape.slot_id, drive.id
            FROM tape LEFT OUTER JOIN drive ON drive.tape_id = tape.id
            WHERE tape.id = \'%s\'"""%(
                tape,))
        res = self.cur.fetchone ()
        self.db.commit()
        return res

        """Get drive info"""
    def get_drive (self, drive):
        self.logger.info("Getting drive %d info")
        self.cur.execute(
            """SELECT * FROM drive LEFT OUTER JOIN tape ON drive.tape_id = tape.id
            WHERE drive.id = %d"""%(
                drive,))
        names = list(map(lambda x: x[0], self.cur.description))
        # print names
        res = self.cur.fetchone ()
        return [names,res]

    def write_buffer_to_tape(self, buffer_dir, drive):
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


    """Get free drives.
    returns a list of free drives, each drive will have a tuple (id, state)"""
    def get_free_drives(self, magazine = None):
        self.logger.info("Getting free drives")
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
        self.logger.info("Getting empty tapes")
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
        # self.logger.info("Loading empty tape to drive %d"%drive)
        res = self.get_empty_tapes()
        self.logger.info ("%d empty tapes"%len(res))
        if len(res) < 1:
            raise Exception("No empty tapes")
        else:
            self.logger.info("Loading empty tape %s to drive %d from slot %d"%(res[0][0], drive, res[0][1]))
            self.load(res[0][1], drive)

        return [['tape_id','drive_id'],res[0]]


    """Load tape from slot to drive"""
    def load (self, slot, drive):
        self.logger.info("Loading tape from slot %d to drive %d"%(slot, drive))
        self.cur.execute(
            """SELECT tape.id
            FROM tape
            WHERE slot_id = %d"""%(
                slot))
        res = self.cur.fetchone ()
        self.logger.debug("Updating drive table for loading")
        self.cur.execute(
            """UPDATE drive
            SET state = 'LOADING', tape_id = '%s'
            WHERE id = %d"""%(
                res[0], drive))
        self.db.commit()
        self.logger.debug("Running command sudo mtx -f /dev/%s load %d %d"%(cnf["controller"], slot, drive))
        cmd=subprocess.Popen(["sudo","mtx","-f","/dev/%s"%cnf["controller"],"load", str(slot), str(drive)], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        self.logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

        self.logger.debug("Updating drive table to IDLE")
        self.cur.execute(
            """UPDATE drive
            set state='IDLE'
            where id=%d"""%(
                drive))
        self.db.commit()
        self.logger.info("Tape loaded, DB updated")

    """Remove tape from drive"""
    def unload(self, drive):
        self.logger.info("Unloading drive %d"%drive)
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

        self.logger.debug("Running command sudo mtx -f /dev/%s unload %d %d"%(cnf["controller"], res[0], drive))
        
        cmd=subprocess.Popen(["sudo","mtx","-f","/dev/%s"%cnf["controller"],"unload", str(res[0]), str(drive)], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        self.logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

        self.cur.execute (
            """UPDATE drive
            SET state = 'EMPTY', tape_id = NULL
            WHERE id = %d"""%(
                drive,))

        self.db.commit()
        self.logger.info ("Drive unloaded, DB updated")

    """Get free slots"""
    def get_free_slots(self):
        self.logger.info("Getting free slots")
        self.cur.execute(
            """SELECT slot.id
            FROM slot LEFT OUTER JOIN tape ON slot.id = tape.slot_id
            WHERE tape.slot_id IS NULL""")
        self.db.commit()
        return self.cur.fetchall()

    """Tar folder to tape in drive.
    Returns the id of the tape in that drive"""
    def tar_folder_to_tape(self, folder, drive):
        self.logger.info("Taring folder %s to tape in drive %d"%(folder, drive))
        self.cur.execute(
            """SELECT attached, tape_id
            FROM drive
            WHERE id = %s"""%(
                drive,))

        res = self.cur.fetchone()
        if res[0] == 1 and res[1] != None:
            size =int(subprocess.check_output(["du","-s",folder]).split()[0])
            self.cur.execute(
                """UPDATE drive
                SET state = 'WRITING', num_writes = num_writes + 1
                WHERE id = %d"""%(
                    drive,))
            self.db.commit()

            path = folder.split("/")
            print "/".join(path[:-1])

            os.chdir("/".join(path[:-1]))
            self.logger.debug("Taring folder %s with size %d bytes to tape %s in drive %d with :\n tar cvf /dev/%s %s"%(
                folder, size, res[1], drive, cnf["drive%s"%drive][0], path[-1]))

            cmd=subprocess.Popen(["sudo", "tar","cvf","/dev/%s"%cnf["drive%s"%drive][0], path[-1]], stdout=subprocess.PIPE)
            cmd.wait()
            comm=cmd.communicate()
            self.logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))

            self.logger.info("Updating  DB")
            
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
            self.logger.info("Committed changes to DB")
        else:
            print "ERROR while writing to drive. Attached = %d, tape = %s"%res

    """Rewind drive"""
    def rewind_drive(self, drive):
        self.logger.info("Rewinding tape in drive %d"%drive)
        self.cur.execute(
                """UPDATE drive
                SET state = 'REWINDING'
                WHERE id = %d"""%(
                    drive,))
        self.db.commit()
        self.logger.debug("Rewinding with command mt -f /dev/%s rewind"%cnf["drive%s"%drive][0] )
        cmd=subprocess.Popen(["sudo", "mt","-f", "/dev/%s"%cnf["drive%s"%drive][0], "rewind"], stdout=subprocess.PIPE)
        cmd.wait()
        comm=cmd.communicate()
        self.logger.debug("The command returned:\n%s\nerror = %s"%(comm[0], str(comm[1])))
        self.cur.execute(
                """UPDATE drive
                SET state = 'IDLE'
                WHERE id = %s"""%(
                    drive,))
        self.db.commit()
        self.logger.info("Committed changes to DB")

    """Take in a drive number and return the files stored on each of the tars on the file.
    Returns a list of strings, string contains all the files in the corresponding tar"""
    def get_file_list (self, drive):
        self.rewind_drive(drive)
        self.cur.execute(
                """UPDATE drive
                SET state = 'READING'
                WHERE id = %d"""%(
                    drive,))
        time.sleep(0.5)
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
        self.rewind_drive(drive)

        self.cur.execute(
                """UPDATE drive
                SET state = 'READING'
                WHERE id = %d"""%(
                    drive,))

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

    def end_of_last_tar (self, drive):
        cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
        cmd.wait()
        while cmd.returncode == 0:
            cmd = subprocess.Popen(["sudo", "mt","-f", "/dev/n%s"%cnf["drive%s"%drive][0], "fsf", "1"], stdout=subprocess.PIPE)
            cmd.wait()



if __name__ == "__main__":
    ta = tape_archive()
    # ta.get_state()
    print ta.write_buffer_to_tape('/var/kat/data/tape_buffer2',1)
    # ta.unload(1)
    # ta.load_tape()
    # 
    # ta.rewind_drive(1)
    # print ta.get_file_list(1)
    # ta.end_of_last_tar(1)
    # ta.tar_folder_to_tape('/home/kat/test_dir', 1)
    # print ta.get_file_list(1)
    # ta.read_file (1,'test_dir/', '/home/kat/katsdpdata/tape_interface/src/dir', tar_num = 1)
    # ta.tar_folder_to_tape('/home/kat/test_tape_write', 1)
    # ta.tar_folder_to_tape('/home/kat/test_dir', 1)
    # ta.tar_folder_to_tape('/home/kat/test_tape_write', 1)
    
    # print ta.print_state()
    ta.close()
