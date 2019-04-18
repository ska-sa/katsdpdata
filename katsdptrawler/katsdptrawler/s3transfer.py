import concurrent.futures
import copy
import logging
import os
import re
import time


from katsdpdata.meerkat_product_extractors import MeerKATTelescopeProductMetExtractor, MeerKATFlagProductMetExtractor
from katsdpdata.met_handler import MetaDataHandler
from katsdpdata.met_detectors import stream_type_detection


from katsdptrawler import s3functions


#logger = logging.getLogger(__name__)


STREAM_TYPES= {'MeerKATTelescopeProduct' : '^[0-9]{10}[-_]sdp[-_](l0$|l0[-_]continuum$)',
               'MeerKATFlagProduct' : '^[0-9]{10}[-_]sdp[-_](l1[-_]flags$|l1[-_]flags[-_]continuum$)'}


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

    def refs_original(self, item):
        raise NotImplementedError

    def transfer(self, transfers):
        logging.debug("Number of workers (processes) for parallel transfer is %i".format(self.workers))
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

    def get(self, name):
        file_loc = os.path.join(self.root, name)
        if os.path.isfile(file_loc):
            return name
        return None

    def refs_original(self, regex=None):
        refs = self.search(regex)
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

    def get(self, name):
        return self.root.get_key(name)

    def refs_original(self, regex=None):
        search = self.search(regex)
        end_point = self.root.connection.host + ':' + str(self.root.connection.port)
        full_path = ['http://' + end_point + '/' + self.root.name + '/']
        for s in search:
            full_path.append('http://' + end_point + '/' + self.root.name + '/' + s.name)
        return full_path

    def get_url(self, ref):
        end_point = self.root.connection.host + ':' + str(self.root.connection.port)
        return ['http://' + end_point + '/' + self.root.name + '/' + ref.name]

class CrawlerBase(object):
    """docstring for CrawlerBase"""
    def __init__(self, regexs):
        self.regexs = regexs
        super(CrawlerBase, self).__init__()

    def _list(self):
        raise NotImplementedError

    def _match_regex(self, contents, regex):
        return sorted([d for d in contents if re.match(regex, d)])

    def list(self):
        contents = self._list()
        matched = {}
        for k,v in self.regexs.items():
            matched[k] = self._match_regex(contents, v)
        return matched


class SourceCrawler(CrawlerBase):
    """docstring for SourceCrawler"""
    def __init__(self):
        regexs = {}
        regexs['head'] = '^[0-9]{10}$'
        regexs['streams'] = '^[0-9]{10}[-_].*$'
        super(SourceCrawler, self).__init__(regexs)


class LocalDirectoryCrawler(SourceCrawler):
    def __init__(self, src):
        self.src = src
        self.root = self.src['config']['trawl_dir']
        super(LocalDirectoryCrawler, self).__init__()

    def _list(self):
        return [d for d in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, d))]


class S3Crawler(SourceCrawler):
    def __init__(self, src):
        self.src = src
        self.root_con = self._s3_connect()
        super(S3Crawler, self).__init__()

    def _s3_connect(self):
        return s3functions.s3_connect(**self.src['config'])

    def _list(self):
        return [b.name for b in self.root_con.get_all_buckets()]


class ProductBase(object):
    """docstring for ProductBase"""
    def __init__(self, name, **kwargs):
        super(ProductBase, self).__init__()
        self.name = name
        self.metadata_id = None
        self._met_handler = None
        if 'solr_endpoint' in kwargs:
            self.set_metadata_handler(kwargs['solr_endpoint'])

    def set_metadata_handler(self, solr_endpoint):
        self._met_handler = MetaDataHandler(solr_endpoint)
        met = self._met_handler.create_core_met(self._product_type(), self.name, self.name)
        self.metadata_id = met['id']

    def _product_type(self):
        raise NotImplementedError

    def _product_extractor(self):
        raise NotImplementedError

    def transferring(self):
        self._met_handler.set_product_transferring(self.metadata_id)

    def received(self):
        self._met_handler.set_product_received(self.metadata_id)

    def product_metadata(self, product_location):
        extractor_class = self._product_extractor()
        met_extractor = extractor_class(product_location)
        met_extractor.extract_metadata()
        self._met_handler.add_prod_met(self.metadata_id, met_extractor.metadata)

    def add_ref_original(self, product_refs):
        self._met_handler.add_ref_original(self.metadata_id, product_refs)

    def add_ref_datastore(self, product_refs):
        self._met_handler.add_ref_datastore(self.metadata_id, product_refs)

class StreamProduct(ProductBase):
    """docstring for StreamProduct"""
    def __init__(self, head, stream, **kwargs):
        super(StreamProduct, self).__init__(stream, **kwargs)
        self.head = head
        self.stream = stream
        rdb = self.stream.replace('-','_') + '.rdb'
        rdb_full = self.stream.replace('-','_') + '.full.rdb'
        self.rdbs = [rdb, rdb_full]
        self.rdb_writing_regex = '^%s$'% (self.stream.replace('-','_') + '\.writing\.' + '*.\.rdb')
        self.npy_regex = '^[a-z_]*.\/[0-9_]*.\.npy$'
        self.npy_writing_regex = '^[a-z_]*.\/[0-9_]*.\.writing.npy$'
        self.complete_token = 'complete'

    def _product_type(self):
        """Given a stream name we need to detect they type for creating metadata for stream products.
        Uses STREAM_TYPES to detect. Supports products of the format:
            (1) 1234567890-sdp-0 == MeerKATTelescopeProduct
            (2) 1234567890-sdp-0-continuum == MeerKATTelescopeProduct
            (3) 1234567890-sdp-1-flags == MeerKATFlagProduct
            (4) 1234567890-sdp-1-flags-continumm == MeerKATFlagProduct

        Parameters
        ----------
        stream_name: string : the name of the stream to detect.
        """
        stream_type = None
        for s_key in STREAM_TYPES.keys():
            if re.match(STREAM_TYPES[s_key], self.name):
                stream_type = s_key
                break
        if not stream_type:
            raise S3TransferError('No product type for %s' % (self.name))
        return stream_type

    def _product_extractor(self):
        stream_type = stream_type_detection(self.name)
        met_extractor = None
        if stream_type == 'MeerKATTelescopeProduct':
            met_extractor = MeerKATTelescopeProductMetExtractor
        elif stream_type == 'MeerKATFlagProduct':
            met_extractor = MeerKATFlagProductMetExtractor
        else:
            raise S3TransferError('No met extractor for %s' % (self.name))
        return met_extractor


class StreamToS3(object):
    def __init__(self, src, sink, stream_product, solr_endpoint):
        super(StreamToS3, self).__init__()
        self.stream_product = stream_product
        self.stream_product.set_metadata_handler(solr_endpoint)
        self.stream_handler = self._init_stream_handler(src, sink)
        self.metadata_handler = self._init_metadata_handler(src, sink)

    def _init_stream_handler(self, src, sink, limit=1000, workers=50):
        # source and sink bucket names
        stream_src = copy.deepcopy(src)
        stream_sink = copy.deepcopy(sink)
        stream_src['bucketname'] = self.stream_product.stream
        stream_sink['bucketname'] = self.stream_product.name
        return self._init_transfer(stream_src, stream_sink, self.stream_product.npy_regex, limit, workers)

    def _init_metadata_handler(self, src, sink, limit=0, workers=1):
        # source and sink bucket names
        meta_src = copy.deepcopy(src)
        meta_sink = copy.deepcopy(sink)
        meta_src['bucketname'] = self.stream_product.head
        meta_sink['bucketname'] = self.stream_product.head
        # TODO: note here that I've put the writing_regex for searches - should not be the case
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
        logging.info('Starting transfer of stream %s.' % (self.stream_product.name))
        self.stream_handler.create_bucket()
        # self.stream_product.transferring()
        complete = False
        while not complete:
            ret = self.stream_handler.run()
            logging.debug('Transferred %i objects %i bytes.' % (ret[0], ret[1]))
            # tripple check to see if the stream transfer is complete
            npys = self.stream_handler.search()
            writing_npys = self.stream_handler.search(self.stream_product.npy_writing_regex)
            complete = self.stream_handler.get(self.stream_product.complete_token)
            if not npys and not writing_npys and complete:
                logging.info('Complete token set for %s, no more transferrable data.' % (self.stream_product.name))
                complete = True
            else:
                logging.debug('Waiting to recheck complete token...')
                time.sleep(1)
                logging.debug('...done.')
        logging.info('Completed transfer of stream %s.' % (self.stream_product.name))

    def metadata_transfer(self):
        def check_rdb(rdb):
            location = None
            rdb_key = None
            while True:
                # local rdb file - extract metadata and transfer
                rdb_key = self.metadata_handler.get(rdb)
                if rdb_key:
                    location = 'source'
                    break
                # remote rdb file - metadata has been extracted
                elif self.metadata_handler.check_key(rdb):
                    location = 'sink'
                    break
                elif self.metadata_handler.search(self.stream_product.rdb_writing_regex):
                    # probably still being written
                    time.sleep(10)
                else:
                    raise S3TransferError('No %s found for %s, something has gone wrong!' %
                         (rdb, self.stream_product.name))
            return (location, rdb_key)

        logging.info('Starting transfer of metadata for %s.' % (self.stream_product.name))
        self.metadata_handler.create_bucket()
        #transfer rdbs
        rdbs = [check_rdb(rdb) for rdb in self.stream_product.rdbs]
        if rdbs[0][0] == 'source':
            prod_url = self.metadata_handler.get_url(rdbs[0][1])
            self.stream_product.product_metadata(prod_url)
        self.metadata_handler.transfer([r[1] for r in rdbs if r[0] == 'source'])

    def run(self):
        self.stream_product.transferring()
        self.stream_transfer()
        # handle metadata
        ret = self.metadata_transfer()
        # handle metadata
        self.stream_product.received()
