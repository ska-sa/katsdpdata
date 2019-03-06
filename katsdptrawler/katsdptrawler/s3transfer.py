import logging
import os
import re

from katsdptrawler import s3functions

logger = logging.getLogger(__name__)


class S3TransferError(Exception):
    """S3 Transfer specific errors"""
    pass


class ItemToS3BucketBase(object):
    """Move a source payload to an s3 bucket with the given keyname. This is a
    base class and should not be instantiated.

    The move method executes sequence: copy, check, delete.
    The copy method executes sequence: copy, check.

    Parameters
    ----------
    bucket: The destination bucket - a boto.s3.bucket.Bucket object.
    keyname: The destination key name to be created in the given bucket.

    Raises
    ------
    S3TransferError
    """
    def __init__(self, bucket, keyname):
        super(ItemToS3BucketBase, self).__init__()
        self.bucket = bucket
        self.keyname = keyname
        self.payload_size = None
        self.sink_size = None

    def _upload(self):
        """Data source specific implementation is required."""
        raise NotImplementedError

    def _delete(self):
        """Data source specific implementation is required."""
        raise NotImplementedError

    def _put(self, payload):
        """Copy the in memory data payload to the destination s3 bucket using the given keyname."""
        key = self.bucket.new_key(self.keyname)
        self.payload_size = len(payload)
        self.sink_size = key.set_contents_from_string(payload)

    def _check(self):
        """A simple size check."""
        if not self.payload_size or not self.sink_size or self.sink_size != self.payload_size:
            raise S3TransferError("%s size is %d while sink size is %d" % (str(self.source), self.payload_size, self.sink_size))
        return True

    def move(self):
        """The method for this class that does the required work. Follows a put,
        get, chevk and delete pattern.

        Returns:
        -------
        The distination, e.g 's3://buckename/keyname;
        """
        payload = self._upload()
        self._put(payload)
        if self._check():
            self._delete()
        return 's3://' + self.bucket.name + '/' + self.keyname

    def copy(self):
        """The method for this class that does the required work. Follows a put,
        check pattern.

        Returns:
        -------
        The distination, e.g 's3://buckename/keyname;
        """
        payload = self._upload()
        self._put(payload)
        self._check()
        return 's3://' + self.bucket.name + '/' + self.keyname

class FileToS3Bucket(ItemToS3BucketBase):
    """Used to move a file from a local filesystem to an S3 bucket.

    Parameters
    ----------
    source: io.BufferedReader : file pointer to open source file.
    bucket: boto.bucket : an open connection to a boto s3 bucket.
    keyname: string : name of the destination key.
    """
    def __init__(self, source, bucket, keyname):
        self.source = source
        super(FileToS3Bucket, self).__init__(bucket, keyname)

    def _upload(self):
        """Call read method on source file pointer."""
        return self.source.read()

    def _delete(self):
       """Unlink the source file."""
       os.unlink(self.source.name)


class KeyToS3Bucket(ItemToS3BucketBase):
    """Used to move files from an s3 key to an S3 bucket.

    Parameters
    ----------
    source: boto key : open boto key.
    bucket: boto.bucket : an open connection to a boto s3 bucket.
    keyname: string : name of the destination key.
    """
    def __init__(self, source, bucket, keyname):
        self.source = source
        super(KeyToS3Bucket, self).__init__(bucket, keyname)

    def _upload(self):
        return self.source.get_contents_as_string()

    def _delete(self):
        self.source.delete()


class ItemsToS3BucketBase(object):
    """Move multiple items from a source to an S3 bucket.

    Parameters
    ----------
    src : dict : contains source configuration information 
    sink : dict : contains destination configuration information
    regex : string : python regex expression to limit search 
    limit : integer : when executing a search, limit the hits
    """
    def __init__(self, src, sink, regex, limit):
        self.src = src
        self.sink = sink
        self.regex = regex
        self.limit = limit

    def _item_iterator(self):
        raise NotImplementedError

    async def transfer(self, transfers):
        raise NotImplementedError

    def create_bucket(self):
        """Create sink S3 bucket."""
        sink_conn = s3functions.s3_connect(**self.sink['config'])
        s3functions.s3_create_bucket(sink_conn, self.sink['bucketname'])
        sink_conn.close()

    def search(self):
        """Search the source using 'self.regex'.
        Return the number of hits, upto 'self.limit'.
        Return all the results if 'self.limit == 0'.

        Return
        ------
        hits : list : all items currently matching 'self.regex' up to 'self.limit'.
        """
        hits = []
        for item in self._item_iterator():
            if re.match(self.regex, item):
                hits.append(item)
            if self.limit != 0 and len(hits) >= self.limit:
                break
        return hits 


class DirContentsToS3Bucket(ItemsToS3BucketBase):
    """Move directory contents to an S3 bucket.

    Parameters
    ----------
    src : dict : contains source configuration information.
                 For example:
                 {'bucketname': 'test-input',
                  'config': {'trawl_dir': '/tmp/test/'}}
    sink : dict : contains destination configuration information
                  For example:
                  {'bucketname': 'test-input',
                   'config': {'host': 'localhost', 'port': 8080, 'profile_name': 'default'}}

    regex : string : python regex expression to limit search 
                  For example to match all files:
                  '^.*$' 
    limit : integer : when executing a search, limit the hits.
                      If set to 0, will return all the hits.
    """
    def __init__(self, src, sink, regex, limit):
        super(DirContentsToS3Bucket, self).__init__(src, sink, regex, limit)
        self.root = os.path.abspath(os.path.join(self.src['config']['trawl_dir'], self.src['bucketname']))

    def _item_iterator(self, root=None):
        if not root:
            root = self.root
        for item in os.scandir(root):
            # symlinks may cause infinite loop
            if item.is_dir(follow_symlinks=False):
                yield from self._item_iterator(item.path)
            else:
                yield os.path.relpath(item.path, self.root)

    async def transfer(self, transfers):
        """Asyncronous method to transfer a list of items given as a parameter.
        Sequence:
            - connect to sink bucket
            - itterate though transfers list, transferring each item 
 
        Parameters
        ----------
        transfers: list : transfers returned from the search method.

        Returns
        -------
        A list of destination item names. For example:
            ['s3://bucketname', 's3://bucketname/keyone', 's3://bucketname/keytwo']
        """
        # configure
        sink_con = s3functions.s3_connect(**self.sink['config'])
        sink_bucket = sink_con.get_bucket(self.sink['bucketname'])

        # work
        transferred = ['s3://' + self.sink['bucketname']]
        for transfer in transfers:
            tname = os.path.join(self.root, transfer)
            with open(tname, 'rb') as t:
                tos3 = FileToS3Bucket(t, sink_bucket, transfer)
                ret = tos3.move()
                transferred.append(ret)
        # cleanup
        sink_con.close()
        return transferred


class BucketContentsToS3Bucket(ItemsToS3BucketBase):
    def __init__(self, src, sink, regex, limit):
        super(BucketContentsToS3Bucket, self).__init__(src, sink, regex, limit)
        self.root_con = s3functions.s3_connect(**self.src['config'])
        self.root = self.root_con.get_bucket(self.src['bucketname'])

    def _item_iterator(self):
        for key in self.root.list():
            yield key.name

    async def transfer(self, transfers):
        """Asyncronous method to transfer a list of items given as a parameter.
        Sequence:
            - connect to source bucket
            - connect to sink bucket
            - itterate though transfers list, transferring each item 
 
        Parameters
        ----------
        transfers: list : transfers returned from the search method.

        Returns
        -------
        A list of destination item names. For example:
            ['s3://bucketname', 's3://bucketname/keyone', 's3://bucketname/keytwo']
        """
        # configure
        src_con = s3functions.s3_connect(**self.src['config'])
        src_bucket = src_con.get_bucket(self.src['bucketname'])

        sink_con = s3functions.s3_connect(**self.sink['config'])
        sink_bucket = sink_con.get_bucket(self.sink['bucketname'])

        # work
        transferred = [self.sink['bucketname']]
        for transfer in transfers:
            src_key = src_bucket.get_key(transfer)
            tos3 = KeyToS3Bucket(src_key, sink_bucket, transfer)
            ret = tos3.move()
            transferred.append(ret)

        # cleanup
        src_con.close()
        sink_con.close()
        return transferred

