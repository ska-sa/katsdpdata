#!/usr/bin/env python

"""Parallel file uploader to trawl NPY files into S3."""

try:
    import futures
except ImportError:
    import concurrent.futures as futures
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

import katsdpservices

import katsdpdata.meerkat_product_extractors
import katsdpdata.met_handler
import katsdpdata.met_detectors

CAPTURE_BLOCK_REGEX = "^[0-9]{10}$"
CAPTURE_STREAM_REGEX = "^[0-9]{10}_.*$"
MAX_TRANSFERS = 5000
CPU_MULTIPLIER = 10
SLEEP_TIME = 20


def main(trawl_dir, boto_dict, solr_url):
    """Main loop for python script. Trawl directory and ingest products into
    archive.  Loop forever, catch any exceptions and continue.

    Parameters
    ----------
    trawl_dir: string : Full path to directory to trawl for products.
    boto_dict: dict : A boto configuration dict.
    sorl_url: string : A solr end point for metadata handeling.
    """
    # test s3 connection
    s3_conn = get_s3_connection(boto_dict)
    s3_conn.close()

    # Outer loop: Trawl forever. Catch all exceptions.
    # Inner loop: Test S3 forever. Catch S3Response error or socket error.
    #            Break back to outer loop on success.
    while True:
        try:
            ret = trawl(trawl_dir, boto_dict, solr_url)
            if ret == 0:
                # if we did not upload anything, probably a good idea to sleep for SLEEP_TIME
                time.sleep(SLEEP_TIME)
        except Exception:
            logger.error("Exception thrown while trawling. Test s3 connection before trawling.")
            while True:
                try:
                    s3_conn = get_s3_connection(boto_dict)
                except Exception:
                    logger.error('Caught exception.')
                    logger.info('Sleeping for %i before continuing.' % (SLEEP_TIME))
                    time.sleep(SLEEP_TIME)
                else:
                    s3_conn.close()
                    break
            continue


def trawl(trawl_dir, boto_dict, solr_url):
    """Main action for trawling a directory for ingesting products
    into the archive.

    Parameters
    ----------
    trawl_dir: string : Full path to directory to trawl for products.
    boto_dict: dict : A boto configuration dict.
    sorl_url: string : A solr end point for metadata handeling.

    Return
    ------
    upload_size: int : The size in bytes of data uploaded. Can be used to
        wait for a set time before trawling directory again.
    """
    cb_dirs, cs_dirs = list_trawl_dir(trawl_dir)
    for cb in sorted(cb_dirs):
        # check for conditions
        cb_files, complete = list_trawl_files(cb, '*.rdb', '*.writing.rdb', 'complete')
        if complete and len(cb_files) == 0:
            cleanup(cb)
        elif len(cb_files) > 1:
            # find all unique products
            rdb_prods = list(set([re.match('^.*[0-9]{10}_[^.]*', cbf).group()
                             for cbf in cb_files
                             if re.match('^.*[0-9]{10}_[^.]*', cbf) is not None]))
            for rdb_prod in rdb_prods:
                rdb_lite, rdb_full = rdb_prod + '.rdb', rdb_prod + '.full.rdb'
                if rdb_lite in cb_files and rdb_full in cb_files:
                    try:
                        prod_met_extractor = katsdpdata.met_detectors.file_type_detection(rdb_lite)
                        met = ingest_vis_product(trawl_dir, os.path.relpath(rdb_prod, cb),
                                                 [rdb_lite, rdb_full], prod_met_extractor, solr_url)
                        logger.info('%s ingested into archive with datastore refs:%s.' %
                                    (met['id'], ', '.join(met['CAS.ReferenceDatastore'])))
                    except Exception as err:
                        if hasattr(err, 'bucket_name'):
                            set_failed_token(os.path.join(trawl_dir, err.bucket_name), str(err))
                        else:
                            raise
    upload_list = []
    for cs in sorted(cs_dirs):
        # check for condtions
        cs_files, complete = list_trawl_files(cs, '*.npy', '*.writing.npy', 'complete')
        if complete and len(cs_files) == 0:
            cleanup(cs)
        elif len(cs_files) > 1:
            upload_list.extend(cs_files)

    # batch upload numpy files
    upload_list = upload_list[0:MAX_TRANSFERS]
    upload_size = sum(os.path.getsize(f)
                      for f in upload_list if os.path.isfile(f))
    if upload_size > 0:
        logger.info("Uploading %.2f MB of data" % (upload_size / 1e6))
        log_time = {}
        proc_results = parallel_upload(trawl_dir, boto_dict, upload_list, log_time=log_time)
        for pr in proc_results:
            try:
                res = pr.result()
                logger.debug("%i transfers from future." % (len(res)))
            except Exception as err:
                # test s3 problems, else mark as borken
                if hasattr(err, 'bucket_name'):
                    set_failed_token(os.path.join(trawl_dir, err.bucket_name), str(err))
        logger.info("Upload complete in %.2f sec (%.2f MBps)" %
                    (log_time['PARALLEL_UPLOAD'], upload_size / 1e6 / log_time['PARALLEL_UPLOAD']))
    else:
        logger.info("No data to upload (%.2f MB)" % (upload_size / 1e6))
    return upload_size


def set_failed_token(prod_dir, msg=None):
    """Set a failed token for the given product directory.

    Parameters
    ----------
    prod_dir: string : The full path to the directory to put the token into.
    msg : string : an optinal message to write into the failed token.
    """
    failed_token_file = os.path.join(prod_dir, "failed")
    if not os.path.isfile(failed_token_file):
        logger.warning("Exception: %s from future." % (msg))
        if not msg:
            msg = ""
        with open(failed_token_file, "w") as failed_token:
            failed_token.write(msg)


def cleanup(dir_name):
    """Recursive delete the supplied directory supplied directory.
    Should be a completed product."""
    logger.info("%s is complete. Deleting directory tree." % (dir_name))
    return shutil.rmtree(dir_name)


def ingest_vis_product(trawl_dir, prod_id, original_refs, prod_met_extractor, solr_url):
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
    except Exception as err:
        bucket_name = os.path.relpath(original_refs[0], trawl_dir).split("/", 1)[0]
        err.bucket_name = bucket_name
        err.filename = original_refs[0]
        raise
    # product metadata extraction
    pm_extractor.extract_metadata()
    mh = katsdpdata.met_handler.MetaDataHandler(solr_url, pm_extractor.product_type, prod_id, prod_id)
    if not mh.get_prod_met(prod_id):
        met = mh.create_core_met()
    else:
        met = mh.get_prod_met(prod_id)
    if "CAS.ProductTransferStatus" in met and met["CAS.ProductTransferStatus"] == "RECEIVED":
        err = katsdpdata.met_extractors.MetExtractorException(
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
    # list all dirs in the trawl_dir
    sub_dirs = [d for d in os.listdir(trawl_dir) if os.path.isdir(os.path.join(trawl_dir, d))]
    # get full path to capture block dirs
    capture_block_dirs = [os.path.join(trawl_dir, d) for d in sub_dirs if re.match(CAPTURE_BLOCK_REGEX, d)]
    # get full path to capture stream dirs
    capture_stream_dirs = [os.path.join(trawl_dir, d) for d in sub_dirs if re.match(CAPTURE_STREAM_REGEX, d)]
    return (capture_block_dirs, capture_stream_dirs,)


def list_trawl_files(prod_dir, file_match, file_writing, complete_token, time_out=10):
    """Return a list of all trawled files in a directory. Files need to
    match the glob pattern. Also, add the complete token if found. Timeout
    after a while, we're going to trim the upload list anyway.

    Parameters
    ----------
    prod_dir: string : The directory to trawl. Usually the sub-directory
        below the top level trawl directory.
    file_match: string : The glob file to match. E.g. '*.npy' or '*.rdb'
    file_writing: string: The glob file to skip since its still being written to.
                          E.g. '*.writing.npy' or '*.writing.rdb'
    complete_token: string : The complete stream complete token to look
        for in the trawled dir.

    Returns
    -------
    file_matches: list : A list of all the matching files for the file glob
        and complete token.
    complete: boolean : True if complete token detected.
    """
    start_time = time.time()
    file_ext = file_match[1:]  # Turn glob into file extension
    write_ext = file_writing[1:]
    file_matches = []
    complete = False
    # check for failed token, if there return an empty list and incomplete.
    if os.path.isfile(os.path.join(prod_dir, "failed")):
        logger.warning("%s so not processing." % (os.path.join(prod_dir, "failed")))
        return ([], False)
    for root, dirnames, filenames in os.walk(prod_dir):
        for filename in filenames:
            if filename.endswith(write_ext):
                # still being written to; ignore
                continue
            elif filename.endswith(file_ext):
                file_matches.append(os.path.join(root, filename))
            elif filename.endswith(complete_token):
                complete = True
            else:
                continue
        time_check = time.time() - start_time
        if time_check > time_out:
            break
    return (file_matches, complete,)


def transfer_files(trawl_dir, boto_dict, file_list):
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
            procs.append(executor.submit(transfer_files, trawl_dir, boto_dict, f))
        executor.shutdown(wait=True)
    return procs


# cut and paste with modification from katsdpmetawriter/scripts/meta_writer.py
def make_boto_dict(s3_args):
    """Create a dict of keyword parameters suitable for passing into a boto.connect_s3 call using the supplied args."""
    return {"host": s3_args.s3_host,
            "port": s3_args.s3_port,
            "is_secure": False,
            "calling_format": boto.s3.connection.OrdinaryCallingFormat()}


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


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("katsdpvistrawler")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <trawl_directory>")
    parser.add_option("--s3-host", default="localhost",
                      help="S3 gateway host address [default = %default]")
    parser.add_option("--s3-port", type="int", default=7480,
                      help="S3 gateway port [default = %default]")
    parser.add_option("--solr-url", default="http://cms3.sdp.mkat.chpc.kat.ac.za:8983/solr/kat_core",
                      help="Solr end point for metadata extraction [default = %default]")

    (options, args) = parser.parse_args()
    if len(args) < 1 or not os.path.isdir(args[0]):
        print(__doc__)
        sys.exit()

    boto_dict = make_boto_dict(options)
    main(trawl_dir=args[0], boto_dict=boto_dict, solr_url=options.solr_url)
