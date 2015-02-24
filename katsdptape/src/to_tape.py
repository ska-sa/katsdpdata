from katcp import BlockingClient, Message

from config import config as cnf

import subprocess
import os

device_host = "192.168.6.233"
device_port = 5000

client = BlockingClient(device_host, device_port)
client.start()
client.wait_protocol() # Optional

reply, informs = client.blocking_request(
    Message.request("sensor-value", "buffer_dir"))
buf = informs[0].arguments[-1]
print reply
print informs
print "-----------"

size = subprocess.check_output(["du","-s",buf]).split()[0]
print size

if size > cnf["soft_tape_limit"]:
    reply, informs = client.blocking_request(
    Message.request("swap-buffer"))
    print reply
    print informs
    print "-----------"

    reply, informs = client.blocking_request(
    Message.request("tar_buffer_to_tape", buf))
    print reply
    print informs
    print "-----------"


client.stop()
client.join()