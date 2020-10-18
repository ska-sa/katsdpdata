#!/usr/bin/python3

import katsdpservices
import logging
import os
import sys

from optparse import OptionParser

from katsdpdata.prod_handler import make_boto_dict
from katsdpdata.prod_handler import download_stream_products


def main(download_dir, capture_block_id, boto_dict, solr_url):
    download_stream_products(download_dir, capture_block_id,
                             solr_url, boto_dict)


if __name__ == "__main__":
    katsdpservices.setup_logging()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("download_cbid_prods")
    katsdpservices.setup_restart()

    parser = OptionParser(usage="download_cbid_prods.py <capture_block_id>")
    parser.add_option("--download-dir", default=os.path.abspath(os.curdir),
                      help="Product download directory [default = %default]")
    parser.add_option("--s3-host", default="archive-gw-1.kat.ac.za",
                      help="S3 gateway host address [default = %default]")
    parser.add_option("--s3-port", type="int", default=7480,
                      help="S3 gateway port [default = %default]")
    parser.add_option("--solr-url",
                      default="http://kat-archive.kat.ac.za:8983/solr/kat_core",
                      help="Solr end point [default = %default]")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        print(__doc__)
        sys.exit()

    boto_dict = make_boto_dict(options)
    main(download_dir=options.download_dir,
         capture_block_id=args[0],
         boto_dict=boto_dict,
         solr_url=options.solr_url)
