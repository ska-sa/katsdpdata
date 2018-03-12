#!/usr/bin/env python

"""Parallel file uploader to trawl NPY files into S3."""

try:
    import concurrent.futures as futures
except:
    import futures
import logging
import multiprocessing
import os
import re
import sys
import socket
import shutil
import time

from optparse import OptionParser

import boto
import boto.s3.connection
import numpy as np

import katsdpservices
import katsdpdata
import katsdpdata.met_extractors
import katsdpdata.met_handler

from katdal.chunkstore_s3 import S3ChunkStore
from katdal.datasources import TelstateDataSource
from katdal.visdatav4 import VisibilityDataV4

CAPTURE_BLOCK_REGEX = '^[0-9]{10}$'
CAPTURE_STREAM_REGEX = '^[0-9]{10}_.*$'
MAX_TRANSFERS = 5000
CPU_MULTIPLIER = 10
S3_STORE='http://10.98.56.13:7480' #'http://archive-gw-1.kat.ac.za:7480'

def main(trawl_dir, boto_dict):
    cb_dirs, cs_dirs = list_trawl_dir(trawl_dir)
    for cb in sorted(cb_dirs):
        #check for conditions
        cb_files, complete  = list_trawl_files(cb, '*.rdb', 'complete' )
        if complete and len(cb_files) == 0:
            cleanup(cb)
        elif len(cb_files) > 1:
            #find all unique products
            rdb_prods = list(set([re.match('^.*[0-9]{10}_[^.]*', cb).group() for cb in cb_files]))
            for rdb_prod in rdb_prods:
                rdb_lite, rdb_full = rdb_prod + '.rdb', rdb_prod + '.full.rdb'
                if rdb_lite in cb_files and rdb_full in cb_files:
                    try:
                        met = ingest_product(trawl_dir, os.path.basename(rdb_lite), [rdb_lite, rdb_full])
                        logger.info('{} ingested into archive with datastore refs:{}.'.
                                     format(met['id'], ', '.join(met['CAS.ReferenceDatastore'])))
                    except Exception as err:
                        if hasattr(err, 'bucket_name'):
                            set_failed_token(os.path.join(trawl_dir, err.bucket_name), str(err))
                        else:
                           raise
    upload_list=[]
    for cs in sorted(cs_dirs):
        #check for condtions
        cs_files, complete  = list_trawl_files(cs, '*.npy', 'complete' )
        if complete and len(cs_files) == 0:
            cleanup(cs)
        elif len(cs_files) > 1:
             upload_list.extend(cs_files)

    #batch upload numpy files
    upload_list = upload_list[0:MAX_TRANSFERS]
    upload_size = sum(os.path.getsize(f)
                      for f in upload_list if os.path.isfile(f))
    if upload_size > 0:
        logger.info("Uploading {} MB of data".format(upload_size / 1e6))
        log_time = {}
        proc_results = parallel_upload(trawl_dir, boto_dict, upload_list, log_time=log_time)
        for pr in proc_results:
            try:
                res = pr.result()
            except Exception as err:
                #test s3 problems, else mark as borken
                if hasattr(err, 'bucket_name'):
                    set_failed_token(os.path.join(trawl_dir, err.bucket_name), str(err))
        logger.info("Upload complete in {}s ({} MBps)".
                    format(log_time['PARALLEL_UPLOAD'], upload_size / 1e6 / log_time['PARALLEL_UPLOAD']))
    else:
         logger.info("No data to upload ({} MB)".format(upload_size / 1e6))
         time.sleep(20)

def set_failed_token(prod_dir, msg):
    failed_token_file = os.path.join(prod_dir, 'failed')
    if not os.path.isfile(failed_token_file):
        logger.warning('Exception: {} from future.'.format(msg))
        with open(failed_token_file, 'w') as failed_token:
            failed_token.write(msg)

def cleanup(dir_name):
    logger.info('{} is complete. Deleting directory tree.'.format(dir_name))
    return shutil.rmtree(dir_name)

def ingest_product(trawl_dir, prod_id, original_refs):
    #get katdal product
    try:
        store = S3ChunkStore.from_url(S3_STORE)
        ds = TelstateDataSource.from_url(original_refs[0], store)
        k = VisibilityDataV4(ds)
    except Exception as err:
        bucket_name = os.path.relpath(original_refs[0], trawl_dir).split('/',1)[0]
        err.bucket_name = bucket_name
        err.filename = original_refs[0]
        raise
    #get metadata extractor
    ks = katsdpdata.met_extractors.MeerKATTelescopeProductMetExtractor(k)
    ks.extract_metadata()
    mh = katsdpdata.met_handler.MetaDataHandler(ks.product_type, prod_id, prod_id)
    if not mh.get_prod_met(prod_id):
        met = mh.create_core_met()
    else:
        met = mh.get_prod_met(prod_id)
    if met.has_key('CAS.ProductTransferStatus') and met['CAS.ProductTransferStatus'] == 'RECEIVED':
        bucket_name = os.path.relpath(original_refs[0], trawl_dir).split('/',1)[0]
        err = katsdpdata.met_extractors.MetExtractorException(
              '{} marked as RECEIVED, while trying to create new product.'.format(prod_id))
        err.bucket_name = bucket_name
        raise err
    #extract metadata
    met = mh.set_product_transferring(met)
    met = mh.add_ref_original(met, original_refs)
    met = mh.add_prod_met(met, ks.metadata)
    transfer_list = parallel_upload(trawl_dir, boto_dict, original_refs)
    met = mh.add_ref_datastore(met, transfer_list)
    met = mh.set_product_received(met)
    return met

def list_trawl_dir(trawl_dir):
    """Creates two lists, that have a valid signature for:
        (1) capture blocks and
        (2) capture streams
    It's useful to seperate out capture blocks and capture stream
    directories, as they are processed differently.

    Parameters
    ----------
    trawl_dir: string : The full path to the trawl directory.

    Returns
    -------
    tuple: (capture_block_dirs, capture_stream_dirs,)
    capture_block_dirs: list : full path to valid capture block directories
    capture_stream_dirs: list : full path to valid capture stream directories
    """
    #list all dirs in the trawl_dir
    sub_dirs = [d for d in os.listdir(trawl_dir) if os.path.isdir(os.path.join(trawl_dir, d))]
    #get full path to capture block dirs
    capture_block_dirs = [os.path.join(trawl_dir, d) for d in sub_dirs if re.match(CAPTURE_BLOCK_REGEX, d)]
    #get full path to capture stream dirs
    capture_stream_dirs = [os.path.join(trawl_dir, d) for d in sub_dirs if re.match(CAPTURE_STREAM_REGEX, d)]
    return (capture_block_dirs, capture_stream_dirs,)


def list_trawl_files(trawl_dir, file_match, complete_token, time_out=10):
    """Return a list of all trawled files in a directory. Files need to
    match the glob pattern. Also, add the complete token if found. Timeout
    after a while, we're going to trim the upload list anyway.

    Parameters
    ----------
    trawl_dir: string : The directory to trawl. Usually the sub-directory
        below the top level trawl directory.
    file_match: string : The glob file to match. E.g. '*.npy' or '*.rdb'
    complete_token: string : The complete stream complete token to look
        for in the trawled dir.

    Returns
    -------
    file_matches: list : A list of all the matching files for the file glob
        and complete token.
    complete: boolean : True if complete token detected.
    """
    start_time = time.time()
    file_ext = file_match[1:] #Turn glob into file extension
    file_matches = []
    complete = False
    #check for failed token, if there return an empty list and incomplete.
    if os.path.isfile(os.path.join(trawl_dir, 'failed')):
        logger.warning('{} so not processing.'.format(os.path.join(trawl_dir, 'failed')))
        return ([], False)
    for root, dirnames, filenames in os.walk(trawl_dir):
        for filename in filenames:
            if filename.endswith(file_ext):
                file_matches.append(os.path.join(root, filename))
            if filename.endswith(complete_token):
                complete = True
        time_check = time.time() - start_time
        if time_check > time_out:
            break
    return (file_matches, complete,)

def transfer_files(trawl_dir, boto_dict, file_list):
    s3_conn = get_s3_connection(boto_dict, fail_on_boto=True)
    bucket = None
    transfer_list = []
    for filename in file_list:
        bucket_name, key_name = os.path.relpath(filename, trawl_dir).split('/',1)
        file_size = os.path.getsize(filename)
        if not bucket or bucket.name != bucket_name:
            bucket = s3_create_bucket(s3_conn, bucket_name)
        if os.path.splitext(filename)[1] == ".npy":
            key_name = os.path.splitext(key_name)[0]
            key = bucket.new_key(key_name)
            #return name of file that caused exception
            try:
                data = np.load(filename).tobytes()
            except Exception as err:
                err.bucket_name = bucket.name
                err.filename = filename
                raise
            data_size = len(data)
            res = key.set_contents_from_string(data)
            if res == data_size:
                os.unlink(filename)
                transfer_list.append('/'.join(['s3:/', bucket.name, key.name]))
            else:
                logger.debug("{} not deleted. Only uploaded {} of {} bytes.".format(filename, res, data_size))
        else:
            key = bucket.new_key(key_name)
            res = key.set_contents_from_filename(filename)
            if res == file_size:
                os.unlink(filename)
                transfer_list.append('/'.join(['s3:/', bucket.name, key.name]))
    return transfer_list

def timeit(func):
    """Taken from an example from the internet."""
    def wrapper(*args, **kwargs):
        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()
        if 'log_time' in kwargs:
            name = kwargs.get('log_name', func.__name__.upper())
            kwargs['log_time'][name] = te - ts
        else:
            logger.info(('{} {} ms').format(func.__name__, (te - ts)))
        return result
    return wrapper

@timeit
def parallel_upload(trawl_dir, boto_dict, file_list, **kwargs):
    workers = CPU_MULTIPLIER * multiprocessing.cpu_count()
    logger.info("Using {} workers".format(workers))
    files = [file_list[i::workers] for i in range(workers)]
    logger.info("Processing {} files".format(len(file_list)))
    procs = []
    with futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for f in files:
            procs.append(executor.submit(transfer_files, trawl_dir, boto_dict,f))
        executor.shutdown(wait=True)
    return procs

#cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def make_boto_dict(s3_args):
    """Create a dict of keyword parameters suitable for passing into a boto.connect_s3 call using the supplied args."""
    return {
            "host": s3_args.s3_host,
            "port": s3_args.s3_port,
            "is_secure": False,
            "calling_format": boto.s3.connection.OrdinaryCallingFormat()
           }

#cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def get_s3_connection(boto_dict, fail_on_boto=False):
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
        logger.error("Failed to connect to S3 host {}:{}. Please check network and host address. ({})".format(s3_conn.host, s3_conn.host.port, e))
    except boto.exception.S3ResponseError as e:
        if e.error_code == 'InvalidAccessKeyId':
            logger.error("Supplied access key {} is not for a valid S3 user.".format(s3_conn.access_key))
        if e.error_code == 'SignatureDoesNotMatch':
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            logger.error("Supplied access key ({}) has no permissions on this server.".format(s3_conn.access_key))
        if fail_on_boto:
            raise
    return None

def s3_create_bucket(s3_conn, bucket_name):
    """Create an s3 bucket, if it fails on a 403 or 409 error, print an error
    message and reraise the exception.
    Returns
    ------
    s3_bucket : boto.s3.bucket.Bucket
        An S3 Bucket object
    """
    try:
        s3_bucket = s3_conn.create_bucket(bucket_name)
    except boto.exception.S3ResponseError as e:
        if e.status == 403 or e.status == 409:
            logger.error("Error status {}. Supplied access key ({}) has no permissions on this server.".format(e.status, s3_conn.access_key))
        raise
    return s3_bucket

if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("katsdpvistrawler")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <trawl_directory>")
    parser.add_option('--s3-host', default='localhost',
                        help='S3 gateway host address [default = %default]')
    parser.add_option('--s3-port', default=7480,
                        help='S3 gateway port [default = %default]')
    (options, args) = parser.parse_args()
    if len(args) < 1 or not os.path.isdir(args[0]):
        print(__doc__)
        sys.exit()

    boto_dict = make_boto_dict(options)
    s3_conn = get_s3_connection(boto_dict, fail_on_boto=True)
    user_id = s3_conn.get_canonical_user_id()
    s3_conn.close()
    logger.info("Successfully tested connection to S3 endpoint as {}.".format(user_id))
    while True:
        main(trawl_dir=args[0], boto_dict=boto_dict)

