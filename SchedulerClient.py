import os
import sys
import zmq
from zmq import ssh
import pickle
import socket
import time

from utils import Message
from utils import RetMessage, bcolors
#===================================================================================================
#++++++Class: SchedulerClient+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#===================================================================================================

class SchedulerClient(object):
    
    def __init__(self, url,tcp_port, local=True):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        if(local):
            self.socket.connect("tcp://127.0.0.1:%s"%tcp_port)
        else:
            try:
                server_name = url.split("@")[0]+"@"+canIHasIP(url.split("@")[1],3)
            except e:
                raise(e)
                return
            #server_name = url.split("@")[0]+"@"+socket.gethostbyname(url.split("@")[1])
            try:
                self.tunnel = ssh.tunnel_connection(self.socket,"tcp://127.0.0.1:%s"%tcp_port, server_name, timeout=3)
            except e:
                raise(e)
                return
        self.socket.setsockopt(zmq.LINGER, 0)
        
        self.poller = zmq.Poller()      
        self.poller.register(self.socket, zmq.POLLIN)
#___________________________________________________________________________________________________    
    def send_msg(self, msg):
        try:
            self.socket.send(msg, zmq.NOBLOCK)
        except:
            print("Failed to send message")
        return_msg = ""

        try:
            while(True):
                socks = dict(self.poller.poll(1000))
                if socks:
                    
                    if socks.get(self.socket) == zmq.POLLIN:
                        return_msg = self.socket.recv(zmq.NOBLOCK)
                        break
                else:
                    print("Timed out")
                    break
        except:
            print("Error while recieveing message")
            
        return return_msg
#___________________________________________________________________________________________________    
    def submit_simple_jobs(self, cmd_list, outlog_path  = None, errlog_path = None, user = "Unknown", env = None, current_dir = None):
        if(outlog_path == None or errlog_path == None):
            msg = Message('SUBMIT_JOBS', 'SIMPLE', user)
            msg.msg["cmd_list"] = cmd_list
            msg.msg["env"] = env
            msg.msg["current_dir"] = current_dir  
        else:
            msg = Message( 'SUBMIT_JOBS', 'SIMPLE_LOG', user)
            msg.msg["cmd_list"] = cmd_list
            msg.msg["env"] = env
            msg.msg["outlog_path"] = outlog_path 
            msg.msg["errlog_path"] = errlog_path
            msg.msg["current_dir"] = current_dir   
            
        #print(msg.msg["current_dir"])
        return self.send_msg(msg.compose())
        
#___________________________________________________________________________________________________    
    def submit_job_description(self, exe, args , outlog  = None, errlog = None, user = "Unknown", env = None):
        msg = Message( 'SUBMIT_JOBS', 'JOB_DESCRIPTION', user)
        msg.msg["executable"] = exe
        msg.msg["args"] = args
        msg.msg["env"] = env
        msg.msg["outlog"] = outlog 
        msg.msg["errlog"] = errlog
                
        return self.send_msg(msg.compose())
#___________________________________________________________________________________________________    
    def classical_submit(self, executable, var, out, err, user, queue, init_dir):
        print("Not implemented yet")
#___________________________________________________________________________________________________
    def request_job_queue(self, opt, user = ""):
        msg = Message('REQUEST_QUEUE', opt, user)
        return self.send_msg(msg.compose())
#___________________________________________________________________________________________________
    def stop_scheduler(self, opt = "NOW", user = ""):
        msg = Message('STOP', opt, user)
        return self.send_msg(msg.compose())
#___________________________________________________________________________________________________
    def change_nproc_limit(self, nproc, user = ""):
        msg = Message('CONFIGURE', 'NPROC', user)
        msg.msg["n-proc"] = nproc
        return self.send_msg(msg.compose())
        
#___________________________________________________________________________________________________
    def get_avg_load(self, user = ""):
        msg = Message('AVG_LOAD', '', user)
        return self.send_msg(msg.compose())    
#___________________________________________________________________________________________________
    def ping(self, user = ""):
        msg = Message('PING', '', user)
        start_time = time.time()
        rec = self.send_msg(msg.compose()).split()
        finish_time = time.time()
        return (rec[1],rec[2],finish_time-start_time)
#___________________________________________________________________________________________________    
    def remove_jobs(self, ids = None, user = "Unknown"):    
        
        if(ids == None):
            opt = 'ALL'
        else:
            opt = 'LIST'
        msg = Message('REMOVE_JOBS', opt, user)
        msg.msg["job_ids"] = ids
        return self.send_msg(msg.compose())
#___________________________________________________________________________________________________    
    def request_job(self, id, user = "Unknown"):
        msg = Message('REQUEST_JOBS', [id], user)
        
        retmsg = self.send_msg(msg.compose())
        #print(retmsg)
        retmsg = pickle.loads(retmsg)#RetMessage()
        #retmsg.decompose(self.send_msg(msg.compose()))
        
        #print(retmsg.status)
        if('error' in retmsg.msg.keys()):
            print(bcolors.BOLD+bcolors.FAIL+retmsg.msg['error']+bcolors.ENDC)
            sys.exit(0)
        return retmsg.msg['job']
#___________________________________________________________________________________________________        
import signal, socket
try:
    import DNS
except:
    DNS = False

#___________________________________________________________________________________________________
def DNSResolve( s ):
    if DNS:
        DNS.ParseResolvConf() # Windows?
        r = DNS.DnsRequest(name=s,qtype='A')
        a = r.req()
        return a.answers[0]['data']
    else:
        return socket.gethostbyname( s )

#___________________________________________________________________________________________________
def dns_timeout(a,b):
    raise Exception("Oh Noes! a DNS lookup timeout!")

#___________________________________________________________________________________________________
def canIHasIP( domain_name, timeout=3 ):
    signal.signal(signal.SIGALRM, dns_timeout)
    signal.alarm( timeout )
    try:
        ip = DNSResolve( domain_name )
    except Exception, exc:
        print exc
        return False
    signal.alarm(0)
    return ip