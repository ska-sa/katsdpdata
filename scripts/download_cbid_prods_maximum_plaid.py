#!/usr/bin/env python3

import boto
import concurrent.futures as futures
import katsdpservices
import logging
import multiprocessing
import os
import pysolr
import sys

from optparse import OptionParser
from katsdpdata.prod_handler import make_boto_dict
from katsdpdata.prod_handler import get_s3_connection

CPU_MULTIPLIER = 10


def parallel_download(download_dir, boto_dict, bucket_name, key_list):
    max_workers = CPU_MULTIPLIER * multiprocessing.cpu_count()
    if len(key_list) < max_workers:
        workers = len(key_list)
    else:
        workers = max_workers
    logger.info("Using %i workers", workers)
    bucket_keys = [key_list[i::workers] for i in range(workers)]
    logger.info("Processing %i files", len(key_list))
    procs = []
    with futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for k in bucket_keys:
            procs.append(
                executor.submit(
                    transfer_files_from_s3, download_dir, boto_dict, bucket_name, k
                )
            )
        executor.shutdown(wait=True)
    return procs


def transfer_files_from_s3(download_dir, boto_dict, bucket_name, bucket_keys):
    s3_conn = get_s3_connection(boto_dict)
    bucket = s3_conn.get_bucket(bucket_name)
    transfer_list = []
    for key in bucket_keys:
        k = bucket.get_key(key)
        download_filename = os.path.join(download_dir, k.bucket.name, k.name)
        if not os.path.isdir(os.path.split(download_filename)[0]):
            os.makedirs(os.path.split(download_filename)[0])
        if not os.path.isfile(download_filename):
            logger.info("Downloading %s", k.name)
            k.get_contents_to_filename(download_filename)
            transfer_list.append(download_filename)
            # TODO: can we confirm the filesize is correct?
        else:
            logger.info("%s exists, skipping.", download_filename)
    return transfer_list


def get_stream_product(download_dir, s3_bucket, boto_dict):
    download_dir = os.path.abspath(download_dir)
    s3_conn = get_s3_connection(boto_dict)

    if s3_bucket.startswith("s3://"):
        bucket_name = os.path.split(s3_bucket)[1]
    else:
        bucket_name = s3_bucket
    bucket = s3_conn.get_bucket(bucket_name)
    for k in bucket:
        download_filename = os.path.join(download_dir, k.bucket.name, k.name)
        if not os.path.isdir(os.path.split(download_filename)[0]):
            os.makedirs(os.path.split(download_filename)[0])
        if not os.path.isfile(download_filename):
            logger.info("Downloading %s", k.name)
            k.get_contents_to_filename(download_filename)
        else:
            logger.info("%s exists, skipping.", download_filename)


def get_capture_block_buckets(capture_block_id, solr_url):
    solr = pysolr.Solr(solr_url)
    search_types = " OR ".join(
        "CAS.ProductTypeName:{}".format(pt)
        for pt in ["MeerKATTelescopeProduct", "MeerKATFlagProduct"]
    )
    return_fields = ", ".join(["CAS.ProductName, CAS.ReferenceDatastore"])
    res = solr.search(
        "CaptureBlockId:{} AND ({})".format(capture_block_id, search_types),
        fl=return_fields,
    )
    s3_buckets = []
    for d in res.docs:
        s3_buckets.append("s3://{}".format(d["CAS.ProductName"]))
        s3_buckets.append(d["CAS.ReferenceDatastore"][0])
    return list(set(s3_buckets))


def download_stream_products_plaid(download_dir, capture_block_id, solr_url, boto_dict):
    bucket_names = get_capture_block_buckets(capture_block_id, solr_url)
    for bn in [b.strip("s3://") for b in bucket_names]:
        s3_conn = get_s3_connection(boto_dict)
        try:
            bucket = s3_conn.get_bucket(bn)
        except boto.exception.S3ResponseError:
            logger.error("Bucket %s does not seem to exist!", bn)
        else:
            bucket_name = bucket.name
            keys = bucket.get_all_keys(max_keys=1000)
            next_marker = keys.next_marker
            parallel_download(
                download_dir, boto_dict, bucket_name, [k.name for k in keys]
            )
            while next_marker:
                logger.info(
                    "Downloading next 1000 keys. Starting from key %s.", next_marker
                )
                keys = bucket.get_all_keys(max_keys=1000, marker=keys.next_marker)
                parallel_download(".", boto_dict, bucket_name, [k.name for k in keys])
                next_marker = keys.next_marker
    logger.info("%s downloaded to %s.", capture_block_id, download_dir)


def main(download_dir, capture_block_id, boto_dict, solr_url):
    download_stream_products_plaid(download_dir, capture_block_id, solr_url, boto_dict)


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("download_cbid_prods")
    katsdpservices.setup_restart()

    parser = OptionParser(
        usage="download_cbid_prods_maximum_plaid.py <capture_block_id>"
    )
    parser.add_option(
        "--download-dir",
        default=os.path.abspath(os.curdir),
        help="Product download directory [default = %default]",
    )
    parser.add_option(
        "--s3-host",
        default="archive-gw-1.kat.ac.za",
        help="S3 gateway host address [default = %default]",
    )
    parser.add_option(
        "--s3-port",
        type="int",
        default=7480,
        help="S3 gateway port [default = %default]",
    )
    parser.add_option(
        "--solr-url",
        default="http://kat-archive.kat.ac.za:8983/solr/kat_core",
        help="Solr end point [default = %default]",
    )

    (options, args) = parser.parse_args()
    if len(args) < 1:
        print(__doc__)
        sys.exit()

    boto_dict = make_boto_dict(options)
    main(
        download_dir=options.download_dir,
        capture_block_id=args[0],
        boto_dict=boto_dict,
        solr_url=options.solr_url,
    )
    logger.info("Download complete!")
