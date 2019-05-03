import concurrent.futures
import copy
import logging
import os
import re
import time

from boto.exception  import S3ResponseError
from katsdpdata.meerkat_product_extractors import MeerKATTelescopeProductMetExtractor, MeerKATFlagProductMetExtractor
from katsdpdata.met_handler import MetaDataHandler
from katsdpdata.met_extractors import MetExtractorException
from katsdpdata.met_detectors import stream_type_detection


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

    def search(self, regex=None):
        raise NotImplementedError

    def get_from_src(self, name):
        raise NotImplementedError

    def put_to_src(self, name, contents):
        raise NotImplementedError

    # TODO: Include a refs_store method.
    def refs_original(self, refs):
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
        sink_con = s3functions.s3_connect(**self.sink['config'])
        s3functions.s3_create_bucket(sink_con, self.sink['bucketname'])
        sink_con.close()

    def check_key(self, keyname):
        check_keyname = None
        sink_con = s3functions.s3_connect(**self.sink['config'])
        sink_bucket = sink_con.get_bucket(self.sink['bucketname'])
        if sink_bucket.get_key(keyname):
           check_keyname = keyname
        sink_con.close()
        return check_keyname

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

    def search(self, regex=None):
        """Search the source using regex. If regex=None
        then search using 'self.regex'
        Return the number of hits, upto 'self.limit'.
        Return all the results if 'self.limit == 0'.

        Parameters
        ----------
        regex : string : regular expression used for searching
                         Example: '^.*$' to match all file names.
        Return
        ------
        hits : list : all items currently matching 'self.regex' up to 'self.limit'.
        """
        if not regex:
            regex = self.regex
        hits = []
        for item in self._item_iterator():
            if re.match(regex, item):
                hits.append(item)
            if self.limit != 0 and len(hits) >= self.limit:
                break
        return hits

    def get_from_src(self, name):
        file_loc = os.path.join(self.root, name)
        if os.path.isfile(file_loc):
            return name
        return None

    def put_to_src(self, name, contents=''):
        file_loc = os.path.join(self.root, name)
        with open(file_loc, 'w') as outfile:
            outfile.write(contents)
        return self.get_url(name)

    def refs_original(self, refs):
        """Make heirachical product references metadata.
        Generated indendenly of checking to see if they exist.

        Parameters
        ----------
        refs: A list of the rdb files

        Returns
        -------
        full_path: A list of the full references.
        For example:
        ['file://dirname/',
         'file://dirname/item1.rdb',
         'file://dirname/item1.full.rdb']
        """
        ret_refs = [self.root]
        for r in refs:
            ret_refs.append(os.path.join(self.root, r))
        return ret_refs

    def get_url(self, ref):
        return 'file://' + os.path.join(self.root, ref)

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

    def search(self, regex=None):
        """Search the source using regex. If regex=None
        then search using 'self.regex'
        Return the number of hits, upto 'self.limit'.
        Return all the results if 'self.limit == 0'.

        Parameters
        ----------
        regex : string : regular expression used for searching
                         Example: '^.*$' to match all file names.
        Return
        ------
        hits : list : all items currently matching 'self.regex' up to 'self.limit'.
        """
        if not regex:
            regex = self.regex
        hits = []
        for item in self._item_iterator():
            if re.match(regex, item.name):
                hits.append(item)
            if self.limit != 0 and len(hits) >= self.limit:
                break
        return hits

    def get_from_src(self, name):
        return self.root.get_key(name)

    def put_to_src(self, name, contents=''):
        new_key = self.root.new_key(name)
        new_key.set_contents_from_string(contents)
        new_key.close()
        return self.get_url(self.name)

    def refs_original(self, refs):
        """Make heirachical product references metadata.
        Generated indendenly of checking to see if they exist.

        Parameters
        ----------
        refs: A list of the rdb files

        Returns
        -------
        full_path: A list of the full references.
        For example:
        ['s3://bucketname/',
         's3://bucketname/item1.rdb',
         's3://buketname/item1.full.rdb']
        """
        end_point = self.root.connection.host + ':' + str(self.root.connection.port)
        full_path = ['s3://' + end_point + '/' + self.root.name + '/']
        for r in refs:
            full_path.append('s3://' + end_point + '/' + self.root.name + '/' + r)
        return full_path

    def get_url(self, ref):
        end_point = self.root.connection.host + ':' + str(self.root.connection.port)
        return ['http://' + end_point + '/' + self.root.name + '/' + ref.name]

class StreamToS3(object):
    def __init__(self, src, sink, stream_product, solr_endpoint):
        super(StreamToS3, self).__init__()
        self.stream_product = stream_product
        self.stream_product.set_metadata_handler(solr_endpoint)
        self._stream_handler = self._init_stream_handler(src, sink)
        self._header_handler = self._init_header_handler(src, sink)

    def _init_stream_handler(self, src, sink, limit=1000, workers=50):
        # source and sink bucket names
        stream_src = copy.deepcopy(src)
        stream_sink = copy.deepcopy(sink)
        stream_src['bucketname'] = self.stream_product.stream
        stream_sink['bucketname'] = self.stream_product.stream
        return self._init_transfer(stream_src, stream_sink, self.stream_product.npy_regex, limit, workers)

    def _init_header_handler(self, src, sink, limit=0, workers=1):
        # source and sink bucket names
        meta_src = copy.deepcopy(src)
        meta_sink = copy.deepcopy(sink)
        meta_src['bucketname'] = self.stream_product.head
        meta_sink['bucketname'] = self.stream_product.head
        return self._init_transfer(meta_src, meta_sink, self.stream_product.rdb_writing_regex, limit, workers)

    def _init_transfer(self, src, sink, regex, limit, workers):
        # create the stream tx according to its source
        if src['type'] == 's3':
            return BucketContentsToS3Bucket(src, sink, regex=regex, limit=limit, workers=workers)
        elif src['type'] == 'dir':
            return DirContentsToS3Bucket(src, sink, regex=regex,  limit=limit, workers=workers)
        else:
            raise S3TransferError('Source type "%s" not supported!' % src['type'])
        return None

    def stream_transfer(self):
        """Transfer a stream source to an s3 sink bucket.

        Loop and search for source files/keys that match
        the npy regex, skipping any writing.npy files. Transfer
        any source files/keys upto a limit.

        Check for complete conditions and loop again if there is more data.

        Once no more source files/keys can be found, the transfer is
        considered complete."""
        logger.info('Starting transfer of stream %s.' % (self.stream_product.name))
        self._stream_handler.create_bucket()
        # self.stream_product.transferring()
        complete = False
        while not complete:
            ret = self._stream_handler.run()
            logger.debug('Transferred %i objects %i bytes.' % (ret[0], ret[1]))
            # tripple check to see if the stream transfer is complete
            npys = self._stream_handler.search(self.stream_product.npy_regex)
            writing_npys = self._stream_handler.search(self.stream_product.npy_writing_regex)
            complete = self._stream_handler.get_from_src(self.stream_product.complete_token)
            if not npys and not writing_npys and complete:
                logger.info('Complete token set for %s, no more transferrable data.' % (self.stream_product.name))
                complete = True
            else:
                logger.debug('Waiting to recheck complete token...')
                time.sleep(1)
                logger.debug('...done.')
        logger.info('Completed transfer of stream %s.' % (self.stream_product.name))

    def header_transfer(self):
        """Header transfers are handeled differently from stream transfers.

        In this context, headers refer to the *.rdb and *.full.rdb files
        created for a visibility data stream.

        Each rdb file is firsly located either as an intermediate file, i.e.
        *.writing.rdb or *.writing.full.rdb  at the source or the sink.

        Files located at source are added to a list and then transfered to the sink.

        This method will loop and wait until the rdb files have successfully
        been completed (by an external process).

        Should these files not be found in either the source or the s3 sink,
        bucket, an exception is raised.
        """
        def rdb_writing(rdb):
            rdb_s = rdb.split('.')
            rdb_ext = '.'.join(rdb_s[1:])
            return '.'.join((rdb_s[0],'writing',rdb_ext))

        def check_rdb_location(rdb):
            if self._header_handler.get_from_src(rdb):
                logger.debug('Found %s at source' % (rdb))
                return ('source', rdb)
            elif self._header_handler.check_key(rdb):
                logger.debug('Found %s at sink' % (rdb))
                return ('sink', rdb)
            elif self._header_handler.search(self.stream_product.rdb_writing_regex):
                raise S3TransferError('%s found afer complete token set, something has gone wrong!' %
                     (rdb_writing(rdb), self.stream_product.name))
            else:
                raise S3TransferError('No %s found for %s, something has gone wrong!' %
                     (rdb, self.stream_product.name))

        logger.info('Starting transfer of header for %s.' % (self.stream_product.name))
        timeout = 10
        self._header_handler.create_bucket()
        while True:
            if self._header_handler.get_from_src(self.stream_product.complete_token):
                rdbs = [check_rdb_location(rdb) for rdb in self.stream_product.rdbs]
                if rdbs[0][0] == 'source' and rdbs[1][0] == 'source':
                    refs_original = self._header_handler.refs_original(self.stream_product.rdbs)
                    self.stream_product.add_ref_original(refs_original)
                break
            else:
                logger.debug('No complete token for %s. Waiting %s' % (self.stream_product.complete_token, timeout))
                time.sleep(timeout)

        if rdbs[0][0] == 'source':
            rdb_url = self._header_handler.get_url(rdbs[0][1])
            logger.info('Extracting product metadata from %s.' % (rdb_url))
            try:
                self.stream_product.product_metadata(rdb_url)
            except MetExtractorException as e:
                logger.info('Exception caught while extracting metadata.')
                logger.info('%s' % (str(e)+'\n'))
                logger.info('Marking product as FAILED')
                self.stream_product.failed()

        # Transfer rdbs from source
        self._header_handler.transfer([r[1] for r in rdbs if r[0] == 'source'])
        # TODO: sort out data store references
        logger.info('Completed transfer of header %s.' % (self.stream_product.name))

    def set_failed(self, error_message):
        self._stream_handler.put_to_src(self.stream_product.failed_token, error_message)
        self._header_handler.put_to_src(self.stream_product.failed_token, error_message)
        self.stream_product.failed()

    def check_failed(self):
        # check for failed token
        if self._header_handler.get_from_src(self.stream_product.failed_token):
            return True
        if self._stream_handler.get_from_src(self.stream_product.failed_token):
            return True
        # check transfer status
        met = self.stream_product._met_handler.get_prod_met(self.stream_product.name)
        if 'CAS.ProductTransferStatus' in met.keys():
           if met['CAS.ProductTransferStatus'] == 'FAILED':
               self.set_failed('FAILED set in CAS.ProductTransferStatus.\n')
               return True
        return False

    def data_transfer(self):
        self.stream_product.transferring()
        self.stream_transfer()
        # rdb transfer and metadata extraction, datastore refs.
        self.header_transfer()
        self.stream_product.received()

    def run(self):
        exit_states = ['complete', 'failed', 'unknown']
        sleep_time = 60
        quashed_errors = [os.errno.ETIMEDOUT, os.errno.EHOSTDOWN, 111, 113]
        if self.check_failed():
            return exit_states[1]

        while True:
            try:
                self.data_transfer()
                return exit_states[0]
            except os.error as e:
                if e.errno == os.errno.ENOENT:
                   self.set_failed(str(e)+'\n')
                   return exit_states[1]
                elif e.errno in quashed_errors:
                    logger.warning("Caught a quashed OSError %s" % e.errno)
                    logger.warning("%s" % (str(e)+'\n'))
                    logger.warning("Waiting %i sec." % (sleep_time))
                    time.sleep(sleep_time)
                else:
                   logger.warning("Caught an unrecognised OSError %s" % e.errno)
                   self.set_failed(str(e)+'n')
                   return exit_states[1]
            except S3ResponseError:
                logger.error("Caught an S3 response error. Waiting %i sec." % (sleep_time))
                time.sleep(sleep_time)
            except Exception as e:
                logger.exception("Caught an unhandled exception. Marking product as failed and exiting. Exception is:\n %s" % (str(e)))
                self.set_failed(str(e)+'\n')
                return exit_states[1]
        # If we return unknown something horrible has happened.
        return exit_states[2]

