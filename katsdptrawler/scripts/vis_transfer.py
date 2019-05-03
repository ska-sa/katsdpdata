#!/usr/bin/env python3
import sys
import katsdptrawler.s3transfer
import katsdpservices
import logging


from optparse import OptionParser


logger = logging.getLogger("katsdpvistrawler.producttransfer")
#logger = logging.basicConfig(stream=sys.stdout, level=logging.INFO)
if __name__ == "__main__":
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()

    parser = OptionParser(usage="product_transfer.py {type} [Options] {header} {stream} \n"+
                                "where type must be either 'dir' or 's3'\n"+
                                "Example:\n"+
                                "product_transfer.py dir [Options] 123456789 123456789_sdp_l0")

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
    header = args[1]
    stream = args[2]

    src = {}
    if src_type == 'dir':
        src['config'] = {'trawl_dir':options.src_trawl_dir}
        src['type'] = 'dir'
    elif src_type == 's3':
        src['config'] = {'host':options.src_host,
                         'port':options.src_port,
                         'profile_name':options.src_profile_name
                        }
        src['type'] = 's3' 
    else:
        logger.info("type != dir or s3. Run benchmark_to_s3.py -h")
        sys.exit(0)
    sink = {}
    sink['config'] = {'host':options.sink_host, 'port':options.sink_port, 'profile_name':options.sink_profile_name}

    product = katsdptrawler.s3transfer.StreamProduct(header, stream)
    tfr = katsdptrawler.s3transfer.StreamToS3(src, sink, product, options.solr_endpoint)
    ret = tfr.run()
    logger.info("%s transfer %s" % (product.name, ret))
    if ret == 'complete':
        sys.exit(0)
    elif ret == 'failure': 
        sys.exit(-1)    
    else:
        sys.exit(-2)
