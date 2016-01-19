from __future__ import division

import errno
import ftplib
import hashlib
import logging
import os
import re
import socket
from socket import error as socket_error
import time

logger = logging.getLogger(__name__)

class AuthenticatedFtpTransfer(object):
    """Class for handling data transfer of files to an ftp server with a
    supplied username and password.
    
    Parmeters
    ---------
    server : string : ftp server address
        Server ip address.
    username : string : ftp server user name for authentication
        Server username.
    passwd : string : ftp server password for authentication
        Server username passsword.
    local_path : string: working directory for transfers
        Local working directory for transfer.
    remote_path : string: ftp server path
        Remote path on ftp server.
    """
    def __init__(self, server, username, password, local_path, remote_path, tx_md5, *args, **kwargs):
        super(AuthenticatedFtpTransfer, self).__init__(*args, **kwargs)
        self.server = server
        self.username = username
        self.password = password
        self.local_path = local_path
        self.remote_path = remote_path.rstrip('/')
        self.tx_md5 = tx_md5

    def connect(self):
        """Autheticated connect to ftp server."""
        logger.info('Opening connection to %s with username:%s password:%s' % (self.server, self.username, self.password,))
        self.ftp = ftplib.FTP(self.server)
        self.ftp.login(user=self.username, passwd=self.password)

    def close(self):
        """Close connection to ftp server."""
        logger.info('Closing FTP connection')
        self.ftp.close()

    def put(self, filename):
        """Transfer a file to ftp server. Calculate MD5 checksum on the fly.
        On a succcessful transfer either delete or transfer to on_success_path.

        Parameters
        ----------
        filename : string : Name of file to transfer
        """
        if self.tx_md5:
            m = hashlib.md5()
            md5_filename = '%s.md5' % (filename,)

        try:
            logger.info('Local path is %s' % (self.local_path,))
            logger.info('Creating remote path %s' % (self.remote_path,))
            self.ftp.mkd(self.remote_path)
            logger.info('Setting permissions to 777 for folder %s' % (self.remote_path,))
            self.ftp.sendcmd('SITE CHMOD 777 ' + (self.remote_path,))
        except ftplib.error_perm as e:
            if e.message[-11:] == 'File exists':
                logger.info('Folder exists, continuing transfer')
            else:
                logger.debug(e)
                logger.info('Exiting')
                exit(0)

        f = open(os.path.join(self.local_path, filename), 'rb')
        hidden_filename = '.%s' % (filename,) #so that we can transfer without alerting the crawler daemon
        try:
            t = time.time()
            logger.info ('Transferring %s as %s' % (filename, hidden_filename,))
            if self.tx_md5:
                self.ftp.storbinary('STOR %s/%s' % (self.remote_path, hidden_filename,), f , blocksize = 128, callback = m.update)
            else:
                self.ftp.storbinary('STOR %s/%s' % (self.remote_path, hidden_filename,), f , blocksize = 128)
            took = time.time() - t
            size_GB = os.fstat(f.fileno()).st_size / 1024 ** 3
            logger.info('Transfer of %d GB took %d secs at rate of %f'%(size_GB, took, size_GB/took))
            logger.info('Granting permissions for %s' % (filename,))
            logger.info(self.ftp.sendcmd('SITE CHMOD 777 %s/%s' % (self.remote_path, hidden_filename,)))
            if self.tx_md5:
                logger.info('Creating md5 file')
                md5_f = open(os.path.join(self.local_path, md5_filename), 'w+', 0)
                logger.info('md5 digest of %s : %s' % (filename, m.hexdigest(),))
                md5_f.write('%s\n' % (m.hexdigest(),))
                # md5_f.flush()
                md5_f.close()  #Otherwise the ftp copy doesn't always have the checksum, even with flush pretty weird
                md5_f = open(os.path.join(self.local_path, md5_filename), 'r')
                logger.info('Sending md5 file')
                self.ftp.storbinary('STOR %s/%s'%(self.remote_path, md5_filename), md5_f)
                logger.info('Granting permissions')
                logger.info(self.ftp.sendcmd('SITE CHMOD 777 %s/%s' % (self.remote_path, md5_filename,)))
                logger.info('Deleting local copy of %s', md5_filename)
                os.remove((os.path.join(self.local_path, md5_filename)))
            logger.info('Updating filename from %s to %s' % (hidden_filename, filename,))
            self.ftp.rename('%s/%s' % (self.remote_path, hidden_filename,), '%s/%s' % (self.remote_path, filename,))
            f.close()
        except socket.timeout as e:
            logger.debug('Connection timed out : %s'%e)
        except ftplib.error_perm as e:
            logger.warning('Do not have permissions for %s on FTP server: %s' % (hidden_filename,e))
            logger.info('Granting permissions to %s' % hidden_filename)
            print self.ftp.sendcmd('SITE CHMOD 777 %s/%s' % (self.remote_path, hidden_filename))
        return filename

class SunStoreTransferDaemon(AuthenticatedFtpTransfer):
    """Class for handling ftp data transfer of files from a local staging directory
    to the ftp server running on sun-store.kat.ac.za.
    
    A local staging directy is periodically checked for files that match 
    the '[0-9]{10}\.h5$' regex expression.

    Parmeters
    ---------
    local_path : string : local staging directory to check periodically
    on_success_path : string: path to move a file to if successfully transfered to ftp server
        Default in None. If set to None files will be deleted.
    regex : string : regular expression for file pattern
        Default set to [0-9]{10}\.h5$
    period : int : sleep time between checking local_path for regex files
        Default is 10 seconds
    """
    def __init__(self, local_path, on_success_path, regex, period, *args, **kwargs):
        super(SunStoreTransferDaemon, self).__init__(server='sun-store.kat.ac.za', username='kat', password='kat', local_path=local_path, remote_path='staging/', tx_md5=True, *args, **kwargs)
        self.on_success_path = on_success_path
        self.regex = re.compile(regex)
        self.period = period

    def run(self):
        """Execution method for class. Periodically check directory for new files to transfer.
        Connect to the ftp server. Initiate a new transfer. Once complete close the ftp connection.
        Sleep. Repeat. Will persist after timeout or hostdown excptions."""
        logger.info('Starting run process')
        logger.info('Watching %s.' % (self.local_path,))
        logger.info('%s' % (os.listdir(self.local_path)))
        while True:
            file_list = [f for f in os.listdir(self.local_path) if os.path.isfile(os.path.join(self.local_path, f)) and self.regex.match(f)]
            import pdb; pdb.set_trace();
            if file_list:
                logger.info('Files in %s to transfer: %s' % (os.path.abspath(self.local_path), ', '.join(file_list),))
                logger.info('Opening connection to %s' % self.server)
                try:
                    self.connect()
                    for f in file_list:
                        put_file = self.put(f)
                        self.cleanup(put_file)
                    self.close()
                except socket_error as serr:
                    if serr.errno in [errno.ETIMEDOUT, errno.EHOSTDOWN]:
                        logger.error(serr)
                        logger.info('Retrying connection to %s' % self.server)
                        continue
                    else:
                        raise serr
            else:
                logger.info('No files to transfer.')
            time.sleep(self.period)

    def cleanup(self, filename):
        """If self.on_success_path is set, move files to that directory, otherwise delete the file."""
        if not self.on_success_path:
            logger.info('Deleting: %s' % (filename))
            os.remove((os.path.join(self.local_path, filename)))
        else:
            logger.info('Moving %s to %s' % (filename, self.on_success_path,))
            os.rename(os.path.join(self.local_path, filename), os.path.join(self.on_success_path, filename))



class SunStoreTransferFile(AuthenticatedFtpTransfer):
    """Class for handling a once off ftp transfer of a file matching filename to the
    sun-store remote directory.

    Parmeters
    ---------
    filename : string : local staging directory to check periodically
    """
    def __init__(self, filename, *args, **kwargs):
        super(SunStoreTransferFile, self).__init__(server='sun-store.kat.ac.za', username='kat', password='kat', local_path=os.path.dirname(os.path.abspath(filename)), remote_path='staging/', tx_md5=True, *args, **kwargs)
        self.filename = os.path.basename(filename)

    def run(self):
        logger.info('Starting transfer process.')
        self.connect()
        self.put(self.filename)
        self.close()
        logger.info('Done.')