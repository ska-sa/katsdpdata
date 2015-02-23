from katcp import DeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Float, Timestamp, Discrete, Int, request, return_reply)

import temp_sensor

server_host = "your computers ip address"
server_port = 5000

class temp_katcp_server(DeviceServer):

    def setup_sensors(self):
        """Setup some server sensors."""

        self._current_temp = Sensor.float("current_temp",
            "Last ?current_temp result.", "")

        self.add_sensor(self._current_temp)

        self._current_temp.set_value(1.0)

    def __init__(self, server_host, server_port):
        DeviceServer.__init__(self, server_host, server_port)
        # self.ta = tape_archive.tape_archive()
        self.set_concurrency_options(False, False)
        signal.signal(signal.SIGINT, signal_handler)


    @request(Int())
    @return_reply(Str())
    def request_read_line(self, req, integer):
        """requests temperature""" #Must have this 
        print integer
        temp_sens = temp_sensor("/dev/ttyACM1", 9600)
        return ('ok',"%d"%temp_sens.read_line())
