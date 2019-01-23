import boto
import boto.s3.connection
import copy
import os
import socket


class S3TransferError(Exception):
    pass


_S3_CONFIG = {
    'profile_name': 'default',
    'host': 'localhost',
    'port': 8080,
    'is_secure': False,
    'calling_format': boto.s3.connection.OrdinaryCallingFormat()
}


def s3_connect(host, port, profile_name='default'):
    """Test the connection to S3 as described in the args, and return
    the current user id and the connection object.
    In general we are more concerned with informing the user why the
    connection failed, rather than raising exceptions. Users should always
    check the return value and make appropriate decisions.
    If set, fail_on_boto will not suppress boto exceptions. Used when verifying
    credentials.
    Returns
    -------
    s3_conn : S3Connection
        A connection to the s3 endpoint. None if a connection error occurred.
    """
    s3_config = copy.deepcopy(_S3_CONFIG)
    s3_config['host'] = host
    s3_config['port'] = port
    s3_config['profile_name'] = profile_name

    s3_conn = boto.connect_s3(**s3_config)
    try:
        s3_conn.get_canonical_user_id()
        # reliable way to test connection and access keys
        return s3_conn
    except socket.error:
        #logger.error("Failed to connect to S3 host %s:%i. Please check network and host address. (%s)" % (s3_conn.host, s3_conn.port, e))
        raise
    except boto.exception.S3ResponseError as e:
        if e.error_code == "InvalidAccessKeyId":
            pass
            #logger.error("Supplied access key %s is not for a valid S3 user." % (s3_conn.access_key))
        if e.error_code == "SignatureDoesNotMatch":
            pass
            #logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            #logger.error("Supplied access key (%s) has no permissions on this server." % (s3_conn.access_key))
            pass
        raise
    return None


class S3TransferBase(object):
    """docstring for S3TransferBase"""
    def __init__(self, bucket, keyname):
        super(S3TransferBase, self).__init__()
        self.bucket = bucket
        self.key = self.bucket.new_key(keyname)
        self.source_size = None
        self.sink_size = None 

    def _copy(self, payload):
        """docstring for _copy"""
        self.source_size = len(payload)
        self.sink_size = self.key.set_contents_from_string(payload)        

    def _check(self):
        """docstring for _check"""
        if not self.source_size or not self.sink_size or self.sink_size != self.source_size:
            raise S3TransferError("%s size is %d while sink size is %d" % (str(self.source), self.source_size, self.sink_size))
        return True
 
    def _delete(self):
        """docstring for _delete"""
        raise NotImplementedError
        
    def transfer(self):
        self._copy()
        if self._check():
            self._delete()


class LocalFiletoS3Transfer(S3TransferBase):
    """docstring for LocalFiletoS3Transfer"""
    def __init__(self, source, bucket, keyname):
        self.source = source
        super(LocalFiletoS3Transfer, self).__init__(bucket, keyname)

    def _copy(self):
        """docstring for _copy"""
        payload = self.source.read()
        super(LocalFiletoS3Transfer, self)._copy(payload)

    def _delete(self):
       """docstring for _delete""" 
       os.unlink(self.source.name)

    def transfer(self): 
        """docstring for transfer"""
        super(LocalFiletoS3Transfer, self).transfer() 
        return ( 'file://%s' % (self.source.name), 's3://%s/%s' % (self.key.bucket.name, self.key.name)) 


class S3toS3Transfer(S3TransferBase):
    """docstring for S3toS3Transfer"""
    def __init__(self, source, bucket, keyname):
        self.source = source
        super(S3toS3Transfer, self).__init__(bucket, keyname)
    
    def _copy(self):
        payload = self.source.get_contents_as_string()
        super(S3toS3Transfer, self)._copy(payload)

    def _delete(self):
        self.source.delete()

    def transfer(self):
        super(S3toS3Transfer, self).transfer()
        return ( 's3://%s/%s' % (self.source.bucket.name, self.source.name), 's3://%s/%s' % (self.key.bucket.name, self.key.name))
