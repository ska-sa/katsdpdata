#!/usr/bin/env python3

"""Parallel file uploader to trawl NPY files into S3."""

import boto
import boto.s3.connection
import concurrent.futures as futures
import multiprocessing
import os
import re
import shutil
import time
from pathlib import PurePath

from katsdpdata.met_extractors import MetExtractorException, file_mime_detection
from katsdpdata.met_handler import MetaDataHandler, ProdMetaDataHandler
from katsdpdata.utilities import get_s3_connection
from katsdpdata.utilities import redact_key
from katsdpdata.utilities import s3_create_anon_access_policy

import logging
logger = logging.getLogger(__name__)

CAPTURE_BLOCK_REGEX = "^[0-9]{10}$"
CAPTURE_STREAM_L0_REGEX = "^[0-9]{10}[-_].*l0$"
CAPTURE_STREAM_L1_REGEX = "^[0-9]{10}[-_].*l1-flags$"
# TODO: This needs to be softcoded
# TODO: https://skaafrica.atlassian.net/browse/SPR1-1111
MAX_TRANSFERS = 5000
CPU_MULTIPLIER = 10
SLEEP_TIME = 20


class Uploader:

    def __init__(self, trawl_dir, boto_dict, upload_files):
        self.trawl_dir = trawl_dir
        self.boto_dict = boto_dict
        self.upload_files = upload_files[:MAX_TRANSFERS]
        self.procs = []

    def s3_create_bucket(self, s3_conn, bucket_name):
        """Create an s3 bucket. If S3CreateError and the error
        status is 409, return a referece to the bucket as it has
        already been created and is owned by you.
        Returns
        ------
        s3_bucket : boto.s3.bucket.Bucket
            An S3 Bucket object
        """
        s3_bucket_policy = s3_create_anon_access_policy(bucket_name)
        try:
            s3_bucket = s3_conn.create_bucket(bucket_name)
            s3_bucket.set_policy(s3_bucket_policy)
        except boto.exception.S3ResponseError as e:
            if e.status == 403 or e.status == 409:
                logger.error(
                    "Error status %s. Supplied access key (%s) "
                    "has no permissions on this server.",
                    e.status, redact_key(s3_conn.access_key))
            raise
        except boto.exception.S3CreateError as e:
            if e.status == 409:  # Bucket already exists and you're the owner
                s3_bucket = s3_conn.get_bucket(bucket_name)
            else:
                raise
        return s3_bucket

    def transfer_files(self, trawl_dir, boto_dict, file_list):
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
            bucket_name, key_name = os.path.relpath(filename, trawl_dir).split(
                "/", 1)
            file_size = os.path.getsize(filename)
            if not bucket or bucket.name != bucket_name:
                bucket = self.s3_create_bucket(s3_conn, bucket_name)
            key = bucket.new_key(key_name)
            res = key.set_contents_from_filename(filename)
            if res == file_size:
                os.unlink(filename)
                transfer_list.append("/".join(["s3:/", bucket.name, key.name]))
            else:
                logger.error(
                    "%s not deleted. Only uploaded %i of %i bytes.",
                    filename, res, file_size)
        return transfer_list

    def upload(self):
        """
        """
        max_workers = CPU_MULTIPLIER * multiprocessing.cpu_count()
        workers = min(len(self.upload_files), max_workers)
        logger.debug("Using %i workers", workers)
        files = [self.upload_files[i::workers] for i in range(workers)]
        logger.debug("Processing %i files", len(self.upload_files))
        self.procs = []  # Clear procs in case we want to re-use this in future
        with futures.ProcessPoolExecutor(max_workers=workers) as executor:
            for f in files:
                self.procs.append(
                    executor.submit(
                        self.transfer_files, self.trawl_dir, self.boto_dict, f))
            executor.shutdown(wait=True)

    def set_failed_tokens(self, solr_url):
        """

        :return:
        """
        for pr in self.procs:
            failed_count = 0
            try:
                res = pr.result()
                logger.debug("%i transfers from future.", len(res))
            except Exception as err:
                # test s3 problems, else mark as broken
                if hasattr(err, 'bucket_name'):
                    product_path = PurePath(self.trawl_dir, err.bucket_name)
                    Product(product_path, solr_url).set_failed_token(str(err))
                    failed_count += 1

    def check_for_multipart(self):
        """Check whether we need have a file bigger than 5GB to make multipart upload
        This will be done in
        https://skaafrica.atlassian.net/browse/SPR1-1114

        :return:
        """
        raise NotImplementedError


class Product:

    def __init__(self, product_path, solr_url):
        """
        :param product_path: string : The directory to trawl. Usually the
                                     sub-directory below the top level
                                     trawl directory.
        """
        self.product_path = product_path
        self.file_matches = []
        self._staged_for_transfer = []
        self.complete = None
        self.met_handler = None
        self.key = self._get_key_from_product_path()
        self.solr_url = solr_url
        self.product_type = ''
        self.product_name = ''  # TODO: This is the same as key

    def _get_key_from_product_path(self):
        raise NotImplementedError

    def set_failed_token(self, msg=None):
        """Set a failed token for the given product directory.

        Parameters
        ----------
        msg : string : an optinal message to write into the failed token.
        """
        failed_token_file = os.path.join(self.product_path, "failed")
        if not os.path.isfile(failed_token_file):
            logger.warning("Exception: %s from future.", msg)
            if not msg:
                msg = ""
            with open(failed_token_file, "w") as failed_token:
                failed_token.write(msg)

    def _discover_trawl_files(
            self, file_match, file_writing, complete_token, time_out=10):
        """Return a list of all trawled files in a directory. Files need to
        match the glob pattern. Also, add the complete token if found. Timeout
        after a while, we're going to trim the upload list anyway.

        Move products with a failed token into the failed directory.

        Parameters
        ----------
        file_match: string : The glob file to match. E.g. '*.npy' or '*.rdb'
        file_writing: string: The glob file to skip since its still being
                             written to. E.g. '*.writing.npy' or '*.writing.rdb'
        complete_token: string : The complete stream complete token to look
            for in the trawled dir.
        time_out: int : Maximum allowed time to perform action

        Returns
        -------
        file_matches: list : A list of all the matching files for the file glob
            and complete token.
        complete: boolean : True if complete token detected.
        """
        prod_dir = os.path.abspath(self.product_path)
        start_time = time.time()
        file_ext = file_match[1:]  # Turn glob into file extension
        write_ext = file_writing[1:]
        # check for failed token, if there return an empty list and incomplete.
        if os.path.isfile(os.path.join(prod_dir, "failed")):
            logger.warning("%s so not processing, moving to failed directory.",
                           os.path.join(prod_dir, "failed"))
            # move product to failed dir
            failed_dir = os.path.join(os.path.split(prod_dir)[0], "failed")
            if not os.path.isdir(failed_dir):
                os.mkdir(failed_dir)
            shutil.move(prod_dir, failed_dir)
            self.update_state('FAILED')
            return
        self.complete = False
        for root, dirnames, filenames in os.walk(prod_dir):
            for filename in filenames:
                if filename.endswith(write_ext):
                    # still being written to; ignore
                    continue
                elif filename.endswith(file_ext):
                    self.file_matches.append(os.path.join(root, filename))
                elif filename.endswith(complete_token):
                    self.complete = True
                else:
                    continue
            time_check = time.time() - start_time
            if time_check > time_out:
                break

    def completed_and_transferred(self):
        """True when the complete token is present and the folder empty.

        :return: boolean : True if transfer is complete else False
        """
        return self.complete and not self.file_matches

    def cleanup(self):
        """Recursive delete the product directory supplied directory.
        Should be a completed product."""
        logger.info("%s is complete. Deleting directory tree.", self.product_path)
        return shutil.rmtree(self.product_path)

    def stage_for_transfer(self, max_parallel_transfers):
        if not max_parallel_transfers:
            self._staged_for_transfer = []
            return 0
        self._staged_for_transfer = self.file_matches[:max_parallel_transfers]
        return max_parallel_transfers - len(self._staged_for_transfer)

    def staged_for_transfer(self):
        return self._staged_for_transfer

    def is_staged(self):
        return bool(self._staged_for_transfer)

    def upload_size(self):
        return sum(
            os.path.getsize(f) for f in self._staged_for_transfer
            if os.path.isfile(f))

    def update_state(self, transition):
        # TODO: Remove all unused logging
        # TODO: Fix this.
        mh = self.met_handler(
            self.solr_url, self.product_type, self.product_name, self.key)
        current_state = mh.get_state()
        if transition == 'TRANSFER_DONE':
            if current_state == 'TRANSFERRING':
                # mh.create_s3_met()
                mh.set_product_status('RECEIVED')
            elif current_state == 'RESTAGING':
                # mh.create_s3_met()
                mh.set_product_status('RESTAGED')
        elif transition == 'PRODUCT_DETECTED':
            if current_state == 'None':
                self.metadata_when_created(mh)
                mh.set_product_status('CREATED')
            elif current_state == 'ARCHIVED':
                self.metadata_when_created(mh)
                mh.set_product_status('RECREATED')
        elif transition == 'TRANSFER_STARTED':
            if not current_state:
                self.metadata_when_created(mh)
                mh.set_product_status('TRANSFERRING')
            elif current_state == 'CREATED':
                mh.set_product_status('TRANSFERRING')
            elif current_state in ['RECREATED', 'ARCHIVED']:
                mh.set_product_status('RESTAGING')
        elif transition == 'FAILED':
            mh.set_product_status('FAILED')

    def metadata_transfer_complete(self, meta_handler):
        # TODO: this should include all the bucket stats we care about
        return {}

    def metadata_when_created(self, meta_handler):
        # TODO: get the set of metadata that needs to be obtained on the
        # TODO: "create" step
        mh = meta_handler()
        mh.create_core_met()

    def bucket_name(self):
        """Get the bucket name from the product_path"""
        if not self.product_path:
            return None
        return [x for x in self.product_path.split(os.sep) if x][-1]


class RDBProduct(Product):
    # Thinking of this as a product might not be technically correct,
    # but it makes this implementation easier.
    regex = CAPTURE_BLOCK_REGEX

    def __init__(self, product_path, solr_url):
        super().__init__(product_path, solr_url)
        self.met_handler = MetaDataHandler

    def _get_key_from_product_path(self):
        name = os.path.split(self.product_path.rstrip('/'))[1]
        return f'{name}_sdp_l0'

    def discover_trawl_files(self):
        super()._discover_trawl_files('*.rdb', '*.writing.rdb', 'complete')

    def get_transfer_list(self):
        transfer_list = []
        for p in self.procs:
            for r in p.result():
                transfer_list.append(r)
        return transfer_list

    def metadata_transfer_complete(self, meta_handler):
        # TODO: ADD THIS BACK
        return {}
        mh = meta_handler
        # procs = self.parallel_upload(trawl_dir, boto_dict, original_refs)
        transfer_list = self.get_transfer_list()
        # prepend the most common path to conform to hierarchical products
        met_transfer_refs = list(transfer_list)
        met_transfer_refs.insert(0, os.path.dirname(
            os.path.commonprefix(transfer_list)))
        met = mh.get_prod_met()
        met = mh.add_ref_datastore(met, met_transfer_refs)
        met = mh.set_product_received(met)
        return met

    def set_rdb_metadata(self, original_refs):
        """Ingest a product into the archive. This includes extracting and uploading
        metadata and then moving the product into the archive.

        Parameters
        ----------
        trawl_dir: string : full path to directory to trawl for ingest product.
        prod_id: string : unique id for the product.
        original_refs : list : list of product file(s).
        prod_met_extractor: class : a metadata extractor class.
        solr_url: string : sorl endpoint for metadata queries and upload.
        boto_dict: dict

        Returns
        -------
        met : dict : a metadata dictionary with uploaded key:value pairs.
        """
        try:
            pm_extractor = file_mime_detection(original_refs[0])
            pm_extractor.extract_metadata()
        except Exception as err:
            err.bucket_name = self.bucket_name()
            raise err
        # Either get the product met or at least create the core meta data
        mh = self.met_handler(
            self.solr_url, pm_extractor.product_type, self.product_name, self.key)
        met = mh.get_prod_met(self.key)
        if not met:
            met = mh.create_core_met()
        if "CAS.ProductTransferStatus" in met and met[
            "CAS.ProductTransferStatus"] == "RECEIVED":
            err = MetExtractorException(
                "%s marked as RECEIVED, while trying to create new product.",
                mh.product_id)
            err.bucket_name = self.bucket_name()
            raise err
        # set metadata
        # met = mh.set_product_transferring(met)
        # prepend the most common path to conform to hierarchical products
        met_original_refs = list(original_refs)
        met_original_refs.insert(0, os.path.dirname(
            os.path.commonprefix(original_refs)))
        met = mh.add_ref_original(met, met_original_refs)
        mh.add_prod_met(met, pm_extractor.metadata)

    def metadata_when_created(self, meta_handler):
        """ When the RDB products are first discovered, we want to set the
        metadata in SOLR.

        TODO: SET STATES TO FAILED WHEN THIS FAILS

        :param meta_handler:
        :return:
        """
        mh = meta_handler
        rdb_prod = self.rdb_file_prefix()
        rdb_lite, rdb_full = f'{rdb_prod}.rdb', f'{rdb_prod}.full.rdb'
        if not (
                rdb_lite in self.file_matches and
                rdb_full in self.file_matches):
            # The RDBs aren't here yet, weird,
            # perhaps we are too quick on the draw?
            return
        try:
            self.set_rdb_metadata([rdb_lite, rdb_full])
        except Exception as err:
            if hasattr(err, 'bucket_name'):
                err.filename = rdb_lite
                logger.exception(
                    "Caught exception while extracting metadata from %s.",
                    err.filename)
                self.set_failed_token(str(err))
                # if failed, set a boolean flag to exit the loop.
                return 'Failed'
            else:
                raise
        return "Completed"

    def rdb_file_prefix(self):
        files = list(set([
            re.match('^.*[0-9]{10}_[^.]*', cbf).group()
            for cbf in self.file_matches
            if re.match('^.*[0-9]{10}_[^.]*', cbf) is not None
        ]))
        return min(files, key=len)


class L0Product(Product):
    regex = CAPTURE_STREAM_L0_REGEX

    def __init__(self, product_path, solr_url):
        super().__init__(product_path, solr_url)
        self.met_handler = ProdMetaDataHandler

    def _get_key_from_product_path(self):
        name = os.path.split(self.product_path.rstrip('/'))[1]
        return f'{name}_sdp_l0_data'

    def discover_trawl_files(self):
        super()._discover_trawl_files('*.npy', '*.writing.npy', 'complete')


class L1Product(Product):
    regex = CAPTURE_STREAM_L1_REGEX

    def __init__(self, product_path, solr_url):
        super().__init__(product_path, solr_url)
        self.met_handler = ProdMetaDataHandler

    def _get_key_from_product_path(self):
        name = os.path.split(self.product_path.rstrip('/'))[1]
        return f'{name}_sdp_l1_flags'

    def discover_trawl_files(self):
        super()._discover_trawl_files('*.npy', '*.writing.npy', 'complete')

    def update_status(self, status):
        pass

    def metadata_transfer_complete(self, meta_handler):
        return {}


class ProductFactory:
    def __init__(self, trawl_dir, solr_url):
        """Creates three lists, that have a valid signature for:
            (1) capture blocks and
            (2) capture l0 streams
            (3) capture l1 streams
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
        self.solr_url = solr_url
        # get full path to capture block dirs
        self.capture_block_dirs = self._list_dir_helper(
            trawl_dir, CAPTURE_BLOCK_REGEX)
        # get full path to capture stream dirs
        # l0
        self.capture_stream_l0_dirs = self._list_dir_helper(
            trawl_dir, CAPTURE_STREAM_L0_REGEX)
        # l1
        self.capture_stream_l1_dirs = self._list_dir_helper(
            trawl_dir, CAPTURE_STREAM_L1_REGEX)

    @staticmethod
    def _list_dir_helper(trawl_dir, regex):
        """List the subdirectories matching the regex

        :param trawl_dir: string : The full path to the trawl directory.
        :param regex: string : The full path to the trawl directory.
        :return: list: full path to directories matching regex
        """
        # list all dirs in the trawl_dir
        sub_dirs = [
            d for d in os.listdir(trawl_dir) if
            os.path.isdir(os.path.join(trawl_dir, d))]
        return [os.path.join(trawl_dir, d) for d in sub_dirs if
                re.match(regex, d)]

    def prune_rdb_products(self):
        """ prune cb_dirs
        this is tested by checking if there are any cs_dirs that start with the cb.
        cb's will only be transferred once all their streams have their
        complete token set.

        :return: int: number of rdb products pruned
        """
        pruned_products = []
        for cb in self.capture_block_dirs[:]:
            for cs in self.capture_stream_l0_dirs:
                if cs.startswith(cb):
                    self.capture_block_dirs.remove(cb)
                    pruned_products.append(cb)
                    break
        for cb in self.capture_block_dirs[:]:
            for cs in self.capture_stream_l1_dirs:
                if cs.startswith(cb):
                    self.capture_block_dirs.remove(cb)
                    pruned_products.append(cb)
                    break
        pruned_count = len(pruned_products)
        self.set_created_on_pruned(pruned_products)
        return pruned_count

    def _get_products_factory(self, product_dirs, product_class):
        return [
            product_class(product_path, self.solr_url)
            for product_path in product_dirs]

    def get_l0_products(self):
        return self._get_products_factory(
            self.capture_stream_l0_dirs, L0Product)

    def get_l1_products(self):
        return self._get_products_factory(
            self.capture_stream_l1_dirs, L1Product)

    def get_rdb_products(self):
        return self._get_products_factory(
            self.capture_block_dirs, RDBProduct)

    def set_created_on_pruned(self, pruned_products):
        # TODO: set the state in the SOLR doc on each of the pruned products
        # TODO: to created
        pass
