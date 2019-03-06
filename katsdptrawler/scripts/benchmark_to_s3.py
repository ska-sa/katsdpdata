#!/usr/bin/env python
import asyncio
import copy
import sys

from katsdptrawler import s3transfer
from optparse import OptionParser

if __name__ == "__main__":
    parser = OptionParser(usage="benchmark_to_s3.py {type} [Options]\n"+
                                 "where type must be either 'dir' or 's3'")

    parser.add_option("--src-search-regex", default="^.*$",
                      help="Regex pattern for searching S3 source bucket [default = %default]")
    parser.add_option("--src-search-limit", type="int", default=0,
                  help="Limit number of hits search returns. 0 returns all results [default = %default]")

    # Dir source 
    parser.add_option("--src-trawl-dir", default="/data/",
                      help="Top level directory [default = %default]")
    parser.add_option("--src-product",
                      help="Directory to transfer [default = %default]")
    # S3 source
    parser.add_option("--src-s3-host", default="localhost",
                      help="Source S3 gateway host address [default = %default]")
    parser.add_option("--src-s3-port", type="int", default=7480,
                      help="Source S3 gateway port [default = %default]")
    parser.add_option("--src-s3-profile-name", default="default",
                      help="Source S3 profile name [default = %default]")
    parser.add_option("--src-s3-bucket-name",
                      help="Source S3 bucket name")

    parser.add_option("--sink-s3-host", default="localhost",
                      help="Sink S3 gateway host address [default = %default]")
    parser.add_option("--sink-s3-port", type="int", default=7480,
                      help="Sink S3 gateway port [default = %default]")
    parser.add_option("--sink-s3-profile-name", default="default",
                      help="Sink Source S3 profile name [default = %default]")
    parser.add_option("--sink-s3-bucket-name",
                      help="Sink S3 bucket name")
    parser.add_option("--parallelism", type="int", default=10,
                      help="Number of parallel transfers to execute")

    (options, args) = parser.parse_args()

    to_s3_class = None
    src = {}

    if 'dir' in args:
        to_s3_class = s3transfer.DirContentsToS3Bucket
        src['bucketname'] = options.src_product
        src['config'] = {'trawl_dir':options.src_trawl_dir}
    elif 's3' in args:
        to_s3_class = s3transfer.BucketContentsToS3Bucket
        src['bucketname'] = options.src_s3_bucket_name
        src['config'] = {'host':options.sink_s3_host, 'port':options.sink_s3_port, 'profile_name':options.sink_s3_profile_name}
    else:
        print("type != dir or s3. Run benchmark_to_s3.py -h")
        sys.exit(0)

    bucket_sink = {}
    bucket_sink['bucketname'] = options.sink_s3_bucket_name
    bucket_sink['config'] = {'host':options.sink_s3_host, 'port':options.sink_s3_port, 'profile_name':options.sink_s3_profile_name}

    #work
    to_s3 = to_s3_class(src, bucket_sink, options.src_search_regex, options.src_search_limit)
    to_s3.create_bucket()
    transfers = to_s3.search()
    loop = asyncio.get_event_loop()
    while len(transfers) > 0:
        p_transfers = [transfers[i::options.parallelism] for i in range(options.parallelism)]
        coros = map(to_s3.transfer, p_transfers)
        group = asyncio.gather(*coros)
        loop.run_until_complete(group)
        transfers = to_s3.search()
    # done
