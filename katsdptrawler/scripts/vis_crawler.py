#!/usr/bin/env python3
import sys
import katsdptrawler.s3transfer
import katsdpservices
import logging


from optparse import OptionParser

logger = logging.getLogger("katsdpvistrawler.producttransfer")

if __name__ == "__main__":
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()

    parser = OptionParser(usage="vis_crawler.py\n" +
                                "Example:\n" +
                                "vis_crawler.py [Options]")

    # Dir source
    parser.add_option("--src-trawl-dir", default="/data/",
                      help="Top level directory [default = %default]")
    # S3 source
    parser.add_option("--src-host", default="localhost",
                      help="Source S3 gateway host address [default = %default]")
    parser.add_option("--src-port", type="int", default=7480,
                      help="Source S3 gateway port [default = %default]")
    parser.add_option("--src-profile-name", default="default",
                      help="Source S3 profile name [default = %default]")

    # S3 sink
    parser.add_option("--sink-host", default="localhost",
                      help="Sink S3 gateway host address [default = %default]")
    parser.add_option("--sink-port", type="int", default=7480,
                      help="Sink S3 gateway port [default = %default]")
    parser.add_option("--sink-profile-name", default="default",
                      help="Sink Source S3 profile name [default = %default]")

    # Solr endpoint
    parser.add_option("--solr-endpoint", default="http://localhost:8983/solr/kat_core",
                      help="Solr endpoint for metadata extraction [default = %default]")

    (options, args) = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)

    src_type = args[0]

    src = {}
    if src_type == 'dir':
        src['config'] = {'trawl_dir': options.src_trawl_dir}
        src['type'] = 'dir'
        crawler = katsdptrawler.s3transfer.LocalDirectoryCrawler(src)
    elif src_type == 's3':
        src['config'] = {'host': options.src_host,
                         'port': options.src_port,
                         'profile_name': options.src_profile_name}
        src['type'] = 's3'
        crawler = katsdptrawler.s3transfer.S3Crawler(src)
    else:
        logger.info("type != dir or s3. Run benchmark_to_s3.py -h")
        sys.exit(0)

sink = {}
sink['config'] = {'host': options.sink_host,
                  'port': options.sink_port,
                  'profile_name': options.sink_profile_name}

prods = crawler.list()

vis_exec = 'scripts/vis_transfer.py %s' % (src['type'])

if src['type'] == 'dir':
    src_conf = '--src-trawl-dir %s' % (src['config']['trawl_dir'])
elif src['type'] == 's3':
    src_conf = '--src-host %s --src-port %s --src-profile-name %s' % (
               src['config']['host'],
               src['config']['port'],
               src['config']['profile_name'])

sink_conf = '--sink-host %s --sink-port %s --sink-profile-name %s' % (
            sink['config']['host'],
            sink['config']['port'],
            sink['config']['profile_name'])

solr_conf = '--solr-endpoint %s' % (options.solr_endpoint)

print('='*80)
for p in prods:
    exec_str = ' '.join([vis_exec, src_conf, sink_conf, solr_conf, p['head'], p['stream']])
    print(exec_str)
print('='*80)
