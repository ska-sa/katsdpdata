"""Interface for the Oracle SL150 tape library"""
import httplib
from socket import socket
import ssl
import requests
import re
import numpy as np

viewstate_regex = re.compile('ViewState" value=".{10,12}">')
ctrlstate_regex = re.compile("'?_adf\.ctrl-state=.{10,14}'") 
slots_regex = re.compile("Slot\s*\d,\w{4,5},\d,\d\s*\(\d{4}\)\s*-\s*Tape:\s*[a-zA-Z0-9]{8}") 
drives_regex = re.compile('"Module \d{1} \w{3,6} Drive \(\d{3}\) [-a-zA-Z0-9 :]+"') 

class tape_interface:
    
    def __init__ (self, ip = "192.168.6.161"):
        self.server_ip = "192.168.6.161"
        self.tapes = []
        self.drives = []
        self.session = requests.Session()
        self.request = self.session.get('https://%s'%self.server_ip, verify = False)
        self.update_viewstate()
        self.update_ctrlstate()

    def update_viewstate (self):
        sp =  viewstate_regex.search(self.request.text).span()
        self.viewstate =  self.request.text[sp[0]: sp[1]][18:-2] # Get viewstate
        print "viewState = %s"%self.viewstate

    def update_ctrlstate (self):
        sp =  ctrlstate_regex.search(self.request.text).span()
        # print r.text[sp[0]: sp[1]]
        self.ctrlstate = self.request.text[sp[0]: sp[1]][16:-1] #Get ctrlstate
        print "ctrlstate = %s"%self.ctrlstate

    def login (self, username="admin", password="passw1rd"):
        payload = {'pt1:it1':'%s'%username,               #login payload
            'pt1:it2': '%s'%password,
            'org.apache.myfaces.trinidad.faces.FORM':'f1',
            'javax.faces.ViewState':self.viewstate,
            'event':'pt1:cb2',
            'event.pt1:cb2':'<m xmlns="http://oracle.com/richClient/comm"><k v="type"><s>action</s></k></m>',
            'oracle.adf.view.rich.PPR_FORCED':'true'}

        self.request = self.session.post('https://%s'%(self.server_ip), data=payload) #login
        self.update_viewstate()
        self.update_ctrlstate()

    def open_modules(self): #The main view opens with only the modeul 1 data available, we need to open the other modules to allows us to access data on those modules
        for i in range(1,5):
            payload = {'org.apache.myfaces.trinidad.faces.FORM':'f1',               #open module payload
                        'javax.faces.ViewState':self.viewstate,
                        'oracle.adf.view.rich.DELTAS':'{pt1:mainPaneRegion:0:mPanelBoxj_id_%i={disclosed=true}}'%i,
                        'event':'pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i,
                        'event.pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i:'<m xmlns="http://oracle.com/richClient/comm"><k v="expand"><b>1</b></k><k v="type"><s>disclosure</s></k></m>',
                        'oracle.adf.view.rich.PROCESS':'pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i}


            self.request = self.session.post('https://%s/faces/TLA?_adf.ctrl-state=%s'%(self.server_ip, self.ctrlstate), data = payload)

    def get_slots(self):
        self.tapes = slots_regex.findall(self.request.text)
        self.tapes = np.unique(self.tapes)

    def get_tapes(self):
        self.drives = drives_regex.findall(self.request.text)
        self.drives = np.unique(self.drives)

    def print_state(self):
        print "ctrlstate = %s"%self.ctrlstate
        print "viewstate = %s"%self.viewstate
        print "____TAPES____"
        for i in self.tapes:
            print i
        print "____DRIVES____"
        for i in self.drives:
            print i

if __name__ == "__main__":
    ti = tape_interface()
    ti.login()
    ti.open_modules()
    ti.get_tapes()
    ti.get_slots()
    ti.print_state()







# serv = httplib.HTTPSConnection(server_ip)
# serv.request("GET","/")
# response = serv.getresponse()
# print """   response :
#     status : %s
#     reason : %s
#     data : %s"""%(str(response.status), str(response.reason), str(response.read()))


# import pdb

# s = requests.Session()  #Start session

# r = s.get('https://%s'%server_ip, verify = False) #Access login
# # print "dir(r) = \n%s"%str(dir(r))
# # # print "cookie = %s"%str(r.cookies)
# # print "json = %s"%str(r.json)
# # print "url = %s"%str(r.url)
# # print "is_redirect? %s"%str(r.is_redirect)
# # print "headers = %s"%str(r.headers)
# # print "request = %s"%str(r.request)
# # print "dir(request)%s"%str(dir(r.request))

# # print "TEXT"
# # print r.text




# # print p.search('<input type="hidden" name="javax.faces.ViewState" value="!15in1soa39">').span()
# sp =  viewstate_regex.search(r.text).span()
# # print r.text[sp[0]: sp[1]]
# viewState =  r.text[sp[0]: sp[1]][18:-2] # Get viewstate
# print "viewState = %s"%viewState



# payload = {'pt1:it1':'admin',               #login payload
#             'pt1:it2': 'passw1rd',
#             'org.apache.myfaces.trinidad.faces.FORM':'f1',
#             'javax.faces.ViewState':viewState,
#             'event':'pt1:cb2',
#             'event.pt1:cb2':'<m xmlns="http://oracle.com/richClient/comm"><k v="type"><s>action</s></k></m>',
#             'oracle.adf.view.rich.PPR_FORCED':'true'}

# r = s.post('https://%s%s'%(server_ip, ''), data=payload) #login



# slot_info_regex = re.compile("Slot\s*\d,\w{4,5},\d,\d\s*\(\d{4}\)\s*-\s*Tape:\s*[a-zA-Z0-9]{8}") #Slot info regex
# drive_info_regex = re.compile('"Module \d{1} \w{3,6} Drive \(\d{3}\) [-a-zA-Z0-9 :]+"')

# print drive_info_regex.findall('alt="Module 1 Top Drive (500) - Bridged - Empty" class="x12')

# # tapes =  slot_info_regex.findall(r.text)
# import numpy as np

# # tapes = np.unique(tapes) #Unique tape info

# # for i in tapes:
# #     print i

# # print len(tapes)

# # ('/TLA','?_adf.ctrl-state=17zbd416yj_14')

# p = re.compile("'?_adf\.ctrl-state=.{10,14}'") #ctrlstate regex

# sp =  p.search(r.text).span()
# # print r.text[sp[0]: sp[1]]
# ctrl_state = r.text[sp[0]: sp[1]][16:-1] #Get ctrlstate
# print "ctrlstate = %s"%ctrl_state
# # print ctrl_state
# sp =  viewstate_regex.search(r.text).span()
# # print r.text[sp[0]: sp[1]]
# viewState =  r.text[sp[0]: sp[1]][18:-2] # Get viewstate
# print "viewState = %s"%viewState

# for i in range(1,5):
#     # sp =  p.search(r.text).span()
#     # ctrl_state = r.text[sp[0]: sp[1]][16:-1] #Get ctrlstate
#     # print "ctrlstate = %s"%ctrl_state
#     payload = {'org.apache.myfaces.trinidad.faces.FORM':'f1',               #open module payload
#                 'javax.faces.ViewState':viewState,
#                 'oracle.adf.view.rich.DELTAS':'{pt1:mainPaneRegion:0:mPanelBoxj_id_%i={disclosed=true}}'%i,
#                 'event':'pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i,
#                 'event.pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i:'<m xmlns="http://oracle.com/richClient/comm"><k v="expand"><b>1</b></k><k v="type"><s>disclosure</s></k></m>',
#                 'oracle.adf.view.rich.PROCESS':'pt1:mainPaneRegion:0:mPanelBoxj_id_%i'%i}


#     r = s.post('https://%s/faces/TLA?_adf.ctrl-state=%s'%(server_ip, ctrl_state), data = payload)
#     # pdb.set_trace()

# tapes =  slot_info_regex.findall(r.text)
# drives = drive_info_regex.findall(r.text)

# drives = np.unique(drives)
# tapes = np.unique(tapes) #Unique tape info

# for i in tapes:
#     print i

# for i in drives:
#     print i

# print len(tapes)

# sp =  viewstate_regex.search(r.text).span()
# # print r.text[sp[0]: sp[1]]
# viewState =  r.text[sp[0]: sp[1]][18:-2] # Get viewstate
# print "viewState = %s"%viewState

# # pdb.set_trace()