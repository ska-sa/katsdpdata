#!/usr/bin/env bash

import katsdpdata.tapeinterface
import logging

from optparse import OptionParser

def parse_args():
    parser = optparse.OptionParser(usage="vis_store_controller.py [options]", description='Run vis store controller')
    parser.add_option("-d", "--database-location", help="Location of the vis-store state database", default = "/var/kat/vis_store_controller")
    parser.add_option("-b", "--buffer-location",  default="/var/kat/vis_store_controller", help="Location of the buffers, will create/use a folder [buffer-location]/buffer1 and [buffer-location]/buffer2")
    parser.add_option("-p", "--port", type="int", default=5000, help="Port for the KATCP server")
    parser.add_option("-s", "--server-ip", default="192.168.6.233", help="IP of the KATCP server")
    parser.add_option("-l", "--log-level", default=logging.WARN , help="Logging level")
    parser.add_option("-f", "--log-file", default="/var/log/vis-store.log" , help="Locaiton of log file")
    (options, args) = parser.parse_args()
    return options

options = get_args()
