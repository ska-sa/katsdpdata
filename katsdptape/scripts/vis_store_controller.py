#!/usr/bin/env python

from katsdptape import TapeDeviceServer

server_host = "192.168.6.233"
server_port = 5000

server = TapeDeviceServers(server_host, server_port)
# server.set_ioloop()
server.start()
server.join()
