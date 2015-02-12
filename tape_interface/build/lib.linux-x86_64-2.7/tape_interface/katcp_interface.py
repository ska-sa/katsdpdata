from katcp import DeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Float, Timestamp, Discrete, request, return_reply)

import threading
import time
import random

import tape_archive

server_host = "192.168.6.66"
server_port = 5000

class tape_katcp_server(DeviceServer):

    VERSION_INFO = ("tape_katcp_interface", 1, 0)
    BUILD_INFO = ("tape_katcp_interface", 0, 1, "")

    # Optionally set the KATCP protocol version and features. Defaults to
    # the latest implemented version of KATCP, with all supported optional
    # features
    # PROTOCOL_INFO = ProtocolFlags(5, 0, set([
    #     ProtocolFlags.MULTI_CLIENT,
    #     ProtocolFlags.MESSAGE_IDS,
    # ]))
    

    def setup_sensors(self):
        """Setup some server sensors."""
        self._add_result = Sensor.float("add.result",
            "Last ?add result.", "", [-10000, 10000])

        self._time_result = Sensor.timestamp("time.result",
            "Last ?time result.", "")

        self._eval_result = Sensor.string("eval.result",
            "Last ?eval result.", "")

        

        self.add_sensor(self._add_result)
        self.add_sensor(self._time_result)
        self.add_sensor(self._eval_result)

    def __init__(self, server_host, server_port):
        DeviceServer.__init__(self, server_host, server_port)
        # self.ta = tape_archive.tape_archive()
        self.set_concurrency_options(False, False)

    @request(Str())
    @return_reply(Str())
    def request_print_state(self, req, table):
        """Print the state of the tape archive.
        Display all with argument 'ALL'.
        Display only tape states with 'TAPE'.
        Display only drive states with 'DRIVE'.
        Display only slots states with 'SLOT'.
        Display only magazine states with 'MAGAZINE'
        """
        ta = tape_archive.tape_archive()
        ret = ""
        if table == "ALL":
            ret = ta.print_state()
        elif table in "TAPE" or table in "DRIVE" or table in "SLOT" or table in "MAGAZINE":
            ta.close()
            return ('fail',"Bad argument %s.\n The options are ALL, TAPE, DRIVE, SLOT and MAGAZINE")
        else:
            ret = ta.print_state(table=table)
        ta.close()
        for line in ret.split("\n"):
            req.inform(line)
        return ('ok', "print-state COMPLETE")

    @request(Str())
    @return_reply(Str())
    def request_echo(self, req, echo_str):
        """Print the state of the tape archive.
        Display all with argument 'ALL'.
        Display only tape states with 'TAPE'.
        Display only drive states with 'DRIVE'.
        Display only slots states with 'SLOT'.
        Display only magazine states with 'MAGAZINE'
        """
        return ('ok', echo_str)


if __name__ == "__main__":
    server = tape_katcp_server(server_host, server_port)
    # server.set_ioloop()
    server.start()
    server.join()