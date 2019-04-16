import concurrent.futures
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
        if self.payload_size is None or self.sink_size is None or self.sink_size != self.payload_size:
            raise S3TransferError("%s size is %d while sink size is %d" % (str(self.source), self.payload_size, self.sink_size))
        return True

    def move(self):
        """The method for this class that does the required work. Follows a put,
        get, chevk and delete pattern.

        Returns:
        -------
        The distination, e.g 's3://buckename/keyname, byte_size;
        """
        payload = self._upload()
        self._put(payload)
        if self._check():
            self._delete()
        return ('s3://' + self.bucket.name + '/' + self.keyname, self.payload_size)

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
        return ('s3://' + self.bucket.name + '/' + self.keyname, self.payload_size)

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
    limit : integer : when executing a search, limit the hits, 0 is no limit
    workers: integer : number of workers for processing pool
    """
    def __init__(self, src, sink, regex, limit, workers):
        self.src = src
        self.sink = sink
        self.regex = regex
        self.limit = limit
        self.workers = workers

    def _item_iterator(self):
        raise NotImplementedError

    def blocked_transfer(self, transfers):
        raise NotImplementedError

    def search(self):
        raise NotImplementedError

    def transfer(self, transfers):
        logger.debug("Number of workers (processes) for parallel transfer is %i".format(self.workers))
        if self.workers == 1:
            return self.blocked_transfer(transfers)
        transfers = [transfers[i::self.workers] for i in range(self.workers)]
        procs = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.workers) as executor:
            for t in transfers:
                procs.append(executor.submit(self.blocked_transfer, t))
            executor.shutdown(wait=True)
        transferred = [procs[0].result()[0][0]]
        transfer_size = 0
        for p in procs:
            ret = p.result()
            transferred.extend(ret[0][1:])
            transfer_size += ret[1]
        return (transferred, transfer_size)

    def create_bucket(self):
        """Create sink S3 bucket."""
        sink_conn = s3functions.s3_connect(**self.sink['config'])
        s3functions.s3_create_bucket(sink_conn, self.sink['bucketname'])
        sink_conn.close()

    def run(self):
        self.create_bucket()
        t = self.search()
        transfer_num = 0
        transfer_size = 0
        while len(t) != 0:
            ret = self.transfer(t)
            transfer_num += len(ret[0][1:])
            transfer_size += ret[1]
            t = self.search()
        return(transfer_num, transfer_size)

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
                  '.*'
    limit : integer : when executing a search, limit the hits.
                      If set to 0, will return all the hits.
    workers : integer : number of workers for processing pool.
                      Defaults is 1 worker.
    """
    def __init__(self, src, sink, regex='.*', limit=0, workers=1):
        super(DirContentsToS3Bucket, self).__init__(src, sink, regex, limit, workers)
        self.root = os.path.abspath(os.path.join(self.src['config']['trawl_dir'], self.src['bucketname']))

    def _item_iterator(self, root=None):
        """Private method for itterating the list. Currently this method cannot
        be in the base class, as it differs from the bucket based implementation.

        Parameters:
        ----------
        root : string : used for recursive calls when hitting a directory.


        Yields:
        -------
        path : string : relpath to the item that has been scanned.
        """
        if not root:
            root = self.root
        for item in os.scandir(root):
            # symlinks may cause infinite loop
            if item.is_dir(follow_symlinks=False):
                yield from self._item_iterator(item.path)
            else:
                yield os.path.relpath(item.path, self.root)

    def blocked_transfer(self, transfers):
        """Blocked method to transfer a list of items given as a parameter.
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
        transfer_size = 0
        for transfer in transfers:
            tname = os.path.join(self.root, transfer)
            with open(tname, 'rb') as t:
                tos3 = FileToS3Bucket(t, sink_bucket, transfer)
                ret = tos3.move()
                transferred.append(ret[0])
                transfer_size += ret[1]
        # cleanup
        sink_con.close()
        return (transferred, transfer_size)

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


class BucketContentsToS3Bucket(ItemsToS3BucketBase):
    """Move bucket contents to an S3 bucket.

    Parameters
    ----------
    src : dict : contains source configuration information
                 For example:
                 {'bucketname': 'test-input',
                  'config': {'host': 'localhost', 'port': 8080, 'profile_name': 'default'}}
    sink : dict : contains destination configuration information
                  For example:
                  {'bucketname': 'test-output',
                   'config': {'host': 'localhost', 'port': 8080, 'profile_name': 'default'}}
    regex : string : python regex expression to limit search
                  For example to match all files:
                  '.*'
    limit : integer : when executing a search, limit the hits.
                      If set to 0, will return all the hits.
    workers : integer : number of workers for processing pool.
                      Defaults is 1 worker.
    """
    def __init__(self, src, sink, regex='.*', limit=0, workers=1):
        super(BucketContentsToS3Bucket, self).__init__(src, sink, regex, limit, workers)
        self.root_con = s3functions.s3_connect(**self.src['config'])
        self.root = self.root_con.get_bucket(self.src['bucketname'])

    def _item_iterator(self):
        """Private method for itterating the list. Currently this method cannot
        be in the base class, as it differs from the dir based implementation.

        Yeilds:
        ------
        key : boto.s3.key.Key : a reference to this type
        """
        for key in self.root.list():
            yield key

    def blocked_transfer(self, transfers):
        """Blocked method to transfer a list of items given as a parameter.
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
        sink_con = s3functions.s3_connect(**self.sink['config'])
        sink_bucket = sink_con.get_bucket(self.sink['bucketname'])

        # work
        transferred = ['s3://' + self.sink['bucketname']]
        transfer_size = 0
        for src_key in transfers:
        #for transfer in transfers:
            #src_key = src_bucket.get_key(transfer)
            tos3 = KeyToS3Bucket(src_key, sink_bucket, src_key.name) #transfer)
            ret = tos3.move()
            transferred.append(ret[0])
            transfer_size += ret[1]
        # cleanup
        # src_con.close()
        sink_con.close()
        return (transferred, transfer_size)

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
            if re.match(self.regex, item.name):
                hits.append(item)
            if self.limit != 0 and len(hits) >= self.limit:
                break
        return hits

