import os
import sys
import zmq
from zmq import ssh
import pickle
import socket
import time

from message import Message,RetMessage

from utils import bcolors
#===================================================================================================
#++++++Class: SchedulerClient+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#===================================================================================================

class SchedulerClient(object):
    
    def __init__(self, socket_name):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("ipc://"+socket_name)
        #if(local):
            #self.socket.connect("tcp://127.0.0.1:%s"%tcp_port)
        #else:
            ##try:
                ##server_name = url.split("@")[0]+"@"+canIHasIP(url.split("@")[1],3)
            ##except e:
                ##raise(e)
                ##return
            ##server_name = url.split("@")[0]+"@"+socket.gethostbyname(url.split("@")[1])
            #server_name = DNSResolve(url)
            #print(server_name)
            #try:
                ##self.socket.connect("tcp://%s:%s"%(server_name,tcp_port))
                #self.tunnel = ssh.tunnel_connection(self.socket,"tcp://127.0.0.1:%s"%tcp_port, server_name, timeout=3)
            #except e:
                #raise(e)
                #return
        self.socket.setsockopt(zmq.LINGER, 0)
        
        self.poller = zmq.Poller()      
        self.poller.register(self.socket, zmq.POLLIN)
#___________________________________________________________________________________________________    
    def send_msg(self, msg):
        return_msg = RetMessage(status = "FAIL")
        try:
            self.socket.send_pyobj(msg, zmq.NOBLOCK)
        except:
            print(bcolors.BOLD+bcolors.FAIL+"Failed to send message"+bcolors.ENDC)

        try:
            while(True):
                socks = dict(self.poller.poll(1000))
                if socks:
                    if socks.get(self.socket) == zmq.POLLIN:
                        constructed_message = False
                        return_msg = self.socket.recv_pyobj(zmq.NOBLOCK)
                        break
                else:
                    print(bcolors.BOLD+bcolors.FAIL+"Timed out"+bcolors.ENDC)
                    break
        except Exception as e:
            print(bcolors.BOLD+bcolors.FAIL+"Error while recieveing message: %s"%e+bcolors.ENDC)
            
        return return_msg
#___________________________________________________________________________________________________    
    def submit_simple_jobs(self, cmd_list, outlog_path  = None, errlog_path = None, user = "Unknown", env = None, wdir = None, shell = True):
        if(outlog_path == None or errlog_path == None):
            msg = Message('SUBMIT_JOBS', ['SIMPLE'], user)
        else:
            msg = Message( 'SUBMIT_JOBS', ['SIMPLE_LOG'], user)
            msg.msg["outlog_path"] = outlog_path 
            msg.msg["errlog_path"] = errlog_path

        msg.msg["cmd_list"] = cmd_list
        msg.msg["env"] = env
        msg.msg["wdir"] = wdir
        msg.msg["shell"] = shell
        
        ret_msg = self.send_msg(msg)
        return ret_msg
#___________________________________________________________________________________________________    
    def submit_job_description(self, exe, args , outlog  = None, errlog = None, user = "Unknown", env = None, shell = False):
        msg = Message( 'SUBMIT_JOBS', ['JOB_DESCRIPTION'], user)
        msg.msg["executable"] = exe
        msg.msg["args"] = args
        msg.msg["env"] = env
        msg.msg["shell"] = shell
        msg.msg["outlog"] = outlog 
        msg.msg["errlog"] = errlog
                
        ret_msg = self.send_msg(msg)
        return ret_msg
#___________________________________________________________________________________________________    
    def classical_submit(self, executable, var, out, err, user, queue, init_dir):
        print("Not implemented yet")
#___________________________________________________________________________________________________
    def stop_scheduler(self, opt = "NOW", user = ""):
        msg = Message('STOP', [opt], user)
        return self.send_msg(msg)
#___________________________________________________________________________________________________
    def change_nproc_limit(self, nproc, user = ""):
        msg = Message('CONFIGURE', ['NPROC'], user)
        msg.msg["n-proc"] = nproc
        return self.send_msg(msg)
        
#___________________________________________________________________________________________________
    def get_avg_load(self, user = ""):
        msg = Message('AVG_LOAD', '', user)
        return self.send_msg(msg)    
#___________________________________________________________________________________________________
    def ping(self, user = ""):
        msg = Message('PING', '', user)
        start_time = time.time()
        rec = self.send_msg(msg).split()
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
        return self.send_msg(msg)
#___________________________________________________________________________________________________    
    def request_job(self, id = None, user = "Unknown"):
        if(id != None):
            msg = Message('REQUEST_JOBS', [id], user)        
        else:
            msg = Message('REQUEST_JOBS', id, user)
            
        retmsg = self.send_msg(msg)
        if('error' in retmsg.msg.keys()):
            print(bcolors.BOLD+bcolors.FAIL+retmsg.msg['error']+bcolors.ENDC)
            sys.exit(0)
        return retmsg.msg['jobs'], retmsg
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