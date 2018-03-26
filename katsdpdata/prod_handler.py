try:
    import futures
except ImportError:
    import concurrent.futures as futures
import logging
import multiprocessing
import os
import pysolr
import socket
import time

import boto
import boto.s3.connection

import met_handler
import met_extractors


logger = logging.getLogger(__name__)


CPU_MULTIPLIER = 10


# cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def make_boto_dict(s3_args):
    """Create a dict of keyword parameters suitable for passing into a boto.connect_s3 call using the supplied args."""
    return {"host": s3_args.s3_host,
            "port": s3_args.s3_port,
            "is_secure": False,
            "calling_format": boto.s3.connection.OrdinaryCallingFormat()}


def s3_create_bucket(s3_conn, bucket_name, bucket_acl="private"):
    """Create an s3 bucket, if it fails on a 403 or 409 error, print an error
    message and reraise the exception.
    Returns
    ------
    s3_bucket : boto.s3.bucket.Bucket
        An S3 Bucket object
    bucket_ack : string : the access control lst to set for the bucket
    """
    valid_acls = ["private", "public-read", "public-read-write", "authenticated-read"]
    default_acl = "private"
    try:
        s3_bucket = s3_conn.create_bucket(bucket_name)
        if bucket_acl in valid_acls:
            s3_bucket.set_acl(bucket_acl)
        else:
            logger.error("Bucket ACL %s, not in %s, setting to %s" % (bucket_acl, valid_acls, default_acl))
            s3_bucket.set_acl(default_acl)
    except boto.exception.S3ResponseError as e:
        if e.status == 403 or e.status == 409:
            logger.error("Error status %s. Supplied access key (%s) has no permissions on this server." % (e.status, s3_conn.access_key))
        raise
    return s3_bucket


# cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def get_s3_connection(boto_dict):
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
    s3_conn = boto.connect_s3(**boto_dict)
    try:
        s3_conn.get_canonical_user_id()
        # reliable way to test connection and access keys
        return s3_conn
    except socket.error as e:
        logger.error("Failed to connect to S3 host %s:%i. Please check network and host address. (%s)" % (s3_conn.host, s3_conn.port, e))
        raise
    except boto.exception.S3ResponseError as e:
        if e.error_code == "InvalidAccessKeyId":
            logger.error("Supplied access key %s is not for a valid S3 user." % (s3_conn.access_key))
        if e.error_code == "SignatureDoesNotMatch":
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            logger.error("Supplied access key (%s) has no permissions on this server." % (s3_conn.access_key))
        raise
    return None



def timeit(func):
    """Taken from an example from the internet."""
    def wrapper(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()
        if "log_time" in kwargs:
            name = kwargs.get("log_name", func.__name__.upper())
            kwargs["log_time"][name] = te - ts
        else:
            logger.info(("%s %.2f ms") % (func.__name__, (te - ts)))
        return result
    return wrapper


@timeit
def parallel_upload(trawl_dir, boto_dict, file_list, **kwargs):
    """
    """
    max_workers = CPU_MULTIPLIER * multiprocessing.cpu_count()
    if len(file_list) < max_workers:
        workers = len(file_list)
    else:
        workers = max_workers
    logger.info("Using %i workers" % (workers))
    files = [file_list[i::workers] for i in range(workers)]
    logger.info("Processing %i files" % (len(file_list)))
    procs = []
    with futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for f in files:
            procs.append(executor.submit(transfer_files_to_s3, trawl_dir, boto_dict, f))
        executor.shutdown(wait=True)
    return procs


def transfer_files_to_s3(trawl_dir, boto_dict, file_list):
    """Transfer file list to s3.

    Parameters
    ----------
    trawl_dir: string : The full path to the trawl directory
    boto_dict: dict : parameter dict for boto connection.
    file_list: list : a list of full path to files to transfer.

    Returns
    -------
    transfer_list: list : a list of s3 URLs that where transfered.
    """
    s3_conn = get_s3_connection(boto_dict)
    bucket = None
    transfer_list = []
    for filename in file_list:
        bucket_name, key_name = os.path.relpath(filename, trawl_dir).split("/", 1)
        file_size = os.path.getsize(filename)
        if not bucket or bucket.name != bucket_name:
            bucket = s3_create_bucket(s3_conn, bucket_name)
        key = bucket.new_key(key_name)
        res = key.set_contents_from_filename(filename)
        if res == file_size:
            os.unlink(filename)
            transfer_list.append("/".join(["s3:/", bucket.name, key.name]))
        else:
            logger.debug("%s not deleted. Only uploaded %i of %i bytes." % (filename, res, file_size))
    return transfer_list


def transfer_files_from_s3(target_dir, key_list):
    """Transfer file list from s3.

    Parameters
    ----------
    trawl_dir: string : The full path to the trawl directory
    boto_dict: dict : parameter dict for boto connection.
    file_list: list : a list of full path to files to transfer.

    Returns
    -------
    transfer_list: list : a list of s3 URLs that where transfered.
    """
    transfer_list = []
    for k in key_list:
        try:
            os.makedirs(os.path.join(os.path.abspath(target_dir), k.bucket.name, os.path.split(k.name)[0]))
        except OSError:
            import pdb; pdb.set_trace()
        filename = os.path.join(os.path.abspath(target_dir), k.bucket.name, k.name)
        k.get_contents_to_filename(filename)
        transfer_list.append('file://{}'.format(filename))
    return transfer_list


def ingest_stream_product(trawl_dir, prod_id, original_refs, prod_met_extractor, solr_url, boto_dict):
    """Ingest a product into the archive. This includes extracting and uploading
    metadata and then moving the product into the archive.

    Parameters
    ----------
    trawl_dir: string : full path to directory to trawl for ingest product.
    prod_id: string : unique id for the product.
    original_refs : list : list of product file(s).
    product_met_extractor: class : a metadata extractor class.
    solr_url: string : sorl endpoint for metadata queries and upload.

    Returns
    -------
    met : dict : a metadata dictionary with uploaded key:value pairs.
    """
    try:
        pm_extractor = prod_met_extractor(original_refs[0])
        pm_extractor.extract_metadata()
    except Exception as err:
        bucket_name = os.path.relpath(original_refs[0], trawl_dir).split("/", 1)[0]
        err.bucket_name = bucket_name
        err.filename = original_refs[0]
        raise
    # product metadata extraction
    mh = met_handler.MetaDataHandler(solr_url, pm_extractor.product_type, prod_id, prod_id)
    if not mh.get_prod_met(prod_id):
        met = mh.create_core_met()
    else:
        met = mh.get_prod_met(prod_id)
    if "CAS.ProductTransferStatus" in met and met["CAS.ProductTransferStatus"] == "RECEIVED":
        err = met_extractors.MetExtractorException(
            "%s marked as RECEIVED, while trying to create new product." % (prod_id))
        err.bucket_name = os.path.relpath(original_refs[0], trawl_dir).split("/", 1)[0]
        raise err
    # set metadata
    met = mh.set_product_transferring(met)
    # prepend the most common path to conform to hierarchical products
    met_original_refs = list(original_refs)
    met_original_refs.insert(0, os.path.dirname(os.path.commonprefix(original_refs)))
    met = mh.add_ref_original(met, met_original_refs)
    met = mh.add_prod_met(met, pm_extractor.metadata)
    procs = parallel_upload(trawl_dir, boto_dict, original_refs)
    transfer_list = []
    for p in procs:
        for r in p.result():
            transfer_list.append(r)
    # prepend the most common path to conform to hierarchical products
    met_transfer_refs = list(transfer_list)
    met_transfer_refs.insert(0, os.path.dirname(os.path.commonprefix(transfer_list)))
    met = mh.add_ref_datastore(met, met_transfer_refs)
    met = mh.set_product_received(met)
    return met


def get_stream_product(download_dir, s3_bucket, boto_dict):
    download_dir = os.path.abspath(download_dir)
    s3_conn = get_s3_connection(boto_dict)

    if s3_bucket.startswith('s3://'):
        bucket_name = os.path.split(s3_bucket)[1]
    else:
        bucket_name = s3_bucket
    bucket = s3_conn.get_bucket(bucket_name)
    for k in bucket:
        download_filename = os.path.join(download_dir, k.bucket.name, k.name)
        if not os.path.isdir(os.path.split(download_filename)[0]):
            os.makedirs(os.path.split(download_filename)[0])
        if not os.path.isfile(download_filename):
            logger.info('Downloading %s' % (k.name))
            k.get_contents_to_filename(download_filename)
        else:
            logger.info('%s exists, skipping.' % (download_filename))


def get_capture_block_buckets(capture_block_id, solr_url):
    solr = pysolr.Solr(solr_url)
    search_types = ' OR '.join('CAS.ProductTypeName:{}'.format(pt) for pt in ['MeerKATTelescopeProduct', 'MeerKATFlagProduct'])
    return_fields = ', '.join(['CAS.ProductName, CAS.ReferenceDatastore'])
    res = solr.search('CaptureBlockId:{} AND ({})'.format(capture_block_id, search_types),
                      fl=return_fields)
    s3_buckets = []
    for d in res.docs:
        s3_buckets.append('s3://{}'.format(d['CAS.ProductName']))
        s3_buckets.append(d['CAS.ReferenceDatastore'][0])
    return list(set(s3_buckets))


def download_stream_products(download_dir, capture_block_id, solr_url, boto_dict):
    bucket_names = get_capture_block_buckets(capture_block_id, solr_url)
    for bn in bucket_names:
        get_stream_product(download_dir, bn, boto_dict)
