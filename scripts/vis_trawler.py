#!/usr/bin/env python3

"""Parallel file uploader to trawl NPY files into S3."""

import boto
import boto.s3.connection
import katsdpservices
import logging
import os
import pysolr
import sys
import socket
import time


from optparse import OptionParser

from katsdpdata.utilities import get_s3_connection
from katsdpdata.utilities import make_boto_dict

from katsdpdata.prod_handler import ProductFactory, Uploader, MAX_TRANSFERS

logger = logging.getLogger(__name__)

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
        except (socket.error, boto.exception.S3ResponseError, pysolr.SolrError):
            logger.error("Exception thrown while trawling. Test solr and s3 connection before continuing.")
            while True:
                try:
                    s3_conn = get_s3_connection(boto_dict)
                    solr_conn = pysolr.Solr(solr_url)
                    solr_conn.search('*:*')
                except Exception as e:
                    logger.debug('Caught exception.')
                    logger.debug('Exception: %s', str(e))
                    logger.debug('Sleeping for %i before continuing.', SLEEP_TIME)
                    time.sleep(SLEEP_TIME)
                else:
                    s3_conn.close()
                    break
            continue
        except Exception:
            logger.exception("Exception thrown while trawling.")
            break


def trawl(trawl_dir, boto_dict, solr_url):
    """Main action for trawqling a directory for ingesting products
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
    product_factory = ProductFactory(trawl_dir, solr_url)
    # TODO: The prune can be dropped after the actual vis products are in SOLR
    # TODO: See https://skaafrica.atlassian.net/browse/SPR1-1113
    total_pruned, pruned_products = product_factory.prune_rdb_products()
    logger.info(
        f'A total of { total_pruned } RDB products will not be transferred this '
        f'cycle, because the corresponding streams have not completed.')
    upload_list = []
    for product in product_factory.get_pruned_products(pruned_products):
        product.solr_url = solr_url
        product.update_state('PRODUCT_DETECTED')

    max_batch_transfers = MAX_TRANSFERS
    for product_list in [
            product_factory.get_rdb_products(),
            product_factory.get_l1_products(),
            product_factory.get_l0_products()]:
        for product in product_list:
            product.discover_trawl_files()
            # TODO: these need to be imported from the envirenoment in future
            product.solr_url = solr_url
            product.boto_dict = boto_dict
            if product.completed_and_transferred():
                product.cleanup()
                product.update_state('TRANSFER_DONE')
            elif product.file_matches:
                product.update_state('PRODUCT_DETECTED')
                max_batch_transfers = product.stage_for_transfer(
                    max_batch_transfers)
                if product.is_staged:
                    upload_list.append(product)

    if not upload_list:
        logger.debug("No data to upload")
        return 0

    upload_size = sum([product.upload_size() for product in upload_list])
    upload_files = []
    for product in upload_list:
        product.update_state('TRANSFER_STARTED')
        upload_files.extend(product.staged_for_transfer)
    logger.debug("Uploading %.2f MB of data", (upload_size // 1e6))
    uploader = Uploader(trawl_dir, boto_dict, upload_files)
    uploader.upload()
    failed_count = uploader.set_failed_tokens(solr_url)
    logger.info(
        f'A total of {failed_count} exceptions where encountered this cycle.')
    return upload_size


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_trawler.py <trawl_directory>")
    parser.add_option("--s3-host", default="localhost",
                      help="S3 gateway host address [default = %default]")
    parser.add_option("--s3-port", type="int", default=7480,
                      help="S3 gateway port [default = %default]")
    parser.add_option("--solr-url", default="http://kat-archive.kat.ac.za:8983/solr/kat_core",
                      help="Solr end point for metadata extraction [default = %default]")

    (options, args) = parser.parse_args()
    if len(args) < 1 or not os.path.isdir(args[0]):
        print(__doc__)
        sys.exit()

    boto_dict = make_boto_dict(options)
    main(trawl_dir=args[0], boto_dict=boto_dict, solr_url=options.solr_url)
