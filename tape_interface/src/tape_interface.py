"""Interface for the Oracle SL150 tape library"""

import httplib
import socket
print socket.ssl

server_ip = "192.168.6.161"
serv = httplib.HTTPSConnection(server_ip)
serv.request("GET","/")
response = serv.getresponse()
print """   response :
    status : %s
    reason : %s
    data : %s"""%(str(response.status), str(response.reason), str(response.read()))