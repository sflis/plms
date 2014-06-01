#!/usr/bin/env python

 
from SchedulerClient import SchedulerClient

from optparse import OptionParser
from os.path import expandvars
import os
import sys
import socket
import utils
import pickle
import re
import collections

SchedulerInfo = collections.namedtuple("SchedulerInfo","name, tcp_addr, tcp_port, host")
class Client(object):
    
    def __init__(self):
        path_here = os.path.dirname(os.path.realpath(__file__))
        self.client_name =  "client_"+socket.gethostname()
        self.scheduler_client = None     
        
        self.conf_file = open(os.path.join(path_here,"plms.conf"),'r')
        
        conf = self.conf_file.readlines()
        self.conf_path = utils.parse(conf,"conf_path")
        self.conf_path_client = utils.parse(conf,"conf_path_client")
        self.state_file = os.path.join(self.conf_path_client, self.client_name+".pkl")
        self.current_scheduler = None
        self.available_schedulers = dict()  
        self.load_state()
 
        self.load_state()
        self.pre_cmd =  {'cs'          :self.pre_cmd_change_sch,
                         'as'          :self.pre_cmd_get_available_sch,
                         'add-remote'  :self.cmd_add_remote,
                         'which'       :self.cmd_which,
                         }
        
        self.commands = {'q'           :self.cmd_print_queue,
                        'stop'        :self.cmd_stop,
                        'rm'          :self.cmd_rm,
                        'submit'      :self.cmd_submit,
                        'submit-list' :self.cmd_submit_list,
                        'avgload'     :self.cmd_avg_load,
                        'submit-jdf'  :self.cmd_submit_jdf,
                        
                        'n-proc'      :self.cmd_cn_proc,
                        'ping'        :self.cmd_ping
                        }
#___________________________________________________________________________________________________
    def load_state(self):
        '''Loads the previus state of the client
        '''
        if(not os.path.isfile(self.state_file)):
            print("No state file found...")
            print("Falling back to local a scheduler:'%s'"%socket.gethostname())
            self.current_scheduler = SchedulerInfo(socket.gethostname(),"127.0.0.1","5555",socket.gethostname())
            self.available_schedulers["%s:%s"%(self.current_scheduler.tcp_addr,self.current_scheduler.name)] = self.current_scheduler   
            return
        state = pickle.load(open(self.state_file))
        self.current_scheduler = state["current_scheduler"]
        self.available_schedulers = state["available_schedulers"]
        
#___________________________________________________________________________________________________
    def save_state(self):
        state = dict()
        state["current_scheduler"] = self.current_scheduler
        state["available_schedulers"] = self.available_schedulers 
        pickle.dump(state, open(self.state_file, 'wb'))
#___________________________________________________________________________________________________
    def initialize_socket(self):
        s = "%s:%s"%(self.current_scheduler.tcp_addr,self.current_scheduler.name)
        s = self.available_schedulers[s]
        if(s.tcp_addr  == "127.0.0.1"):
            self.scheduler_client = SchedulerClient(s.tcp_addr,s.tcp_port)
        else:
            self.scheduler_client = SchedulerClient(s.tcp_addr,s.tcp_port, local = False)
#___________________________________________________________________________________________________
    def test_connection(self):
        pass
#___________________________________________________________________________________________________
    def pre_cmd_get_available_sch(self,arg, opt):
        print("here")
        for k in self.available_schedulers.keys():
            print("%30s | %s"%(k,self.available_schedulers[k]))
#___________________________________________________________________________________________________
    def cmd_cn_proc(self, arg,opt):
        self.scheduler_client.change_nproc_limit(int(opt[0]))
#___________________________________________________________________________________________________
    def cmd_avg_load(self,arg,opt):
        print(self.scheduler_client.get_avg_load())
#___________________________________________________________________________________________________
    def pre_cmd_change_sch(self,arg, opt):
        self.current_scheduler = self.available_schedulers[opt[0]]
#___________________________________________________________________________________________________
    def cmd_print_queue(self,arg, opt):
        if(opt == None):
            queue = self.scheduler_client.request_job_queue("RQ")
        else:
            queue = self.scheduler_client.request_job_queue(opt[0])
            
        for l in queue.splitlines()[2:]:
            print(l)
#___________________________________________________________________________________________________
    def cmd_stop(self,arg, opt):
        if(opt[0] == "now"):
            print(self.scheduler_client.stop_scheduler("NOW"))
        elif(opt[0] == "gentle"):
            print(self.scheduler_client.stop_scheduler("GENTLE"))
        else:
            print("Error") 
#___________________________________________________________________________________________________
    def cmd_rm(self, arg, opt):
        if(opt == None):
            yn = raw_input("Are you sure to remove all jobs in queue [yes or no]? ")
            while(yn != "yes" and yn != "no"):
                print(yn)
                yn = raw_input("Pleas type 'yes' or 'no': ")
            if(yn == "yes"):
                self.scheduler_client.remove_jobs(None)
        else:
            ids = [int(i) for i in opt]
            print("removed: "+self.scheduler_client.remove_jobs(ids)+"jobs")
#___________________________________________________________________________________________________
    def cmd_submit(self,arg, opt):
        if(options == None):
            print("Must pass a command to submit")
        else:
            return_msg = self.scheduler_client.submit_simple_jobs([" ".join(options)], env = os.environ)
            print(return_msg)
        if(return_msg.find("FAIL")>=0):
            print("Submition failed")
        else:
            print("Submited job") 
#___________________________________________________________________________________________________
    def cmd_submit_list(self,arg, opt):
        if(opt == None):
            print("Error")
        else:
            joblist = open(opt[0],'r').readlines()
            self.scheduler_client.submit_simple_jobs(joblist, env = os.environ)	
            
#___________________________________________________________________________________________________
    def cmd_ping(self,arg, opt):
        (name,host,dt) = self.scheduler_client.ping()
        print("Ping: %f"%dt)
        print("Host: %s, Name: %s"%(host,name))
#___________________________________________________________________________________________________
    def cmd_submit_jdf(self, arg, opt):
        jdf_file = open(opt[0],'r')
        jdf = jdf_file.readlines()
        exe = utils.parse(jdf,"Executable",separator='=')
        args = utils.parse(jdf,"Args",separator='=',complete_line=True)
        use_env = utils.parse(jdf,"EnvPass",separator='=')
        out = utils.parse(jdf,"Out",separator='=')
        err = utils.parse(jdf,"Err",separator='=')
        
        if(use_env == "true"):
            env = os.environ
        else:
            env = None
        
        self.scheduler_client.submit_job_description(exe, args , out , err, "Unknown", env)
#___________________________________________________________________________________________________
    def cmd_which(self, arg, opt):
        if(self.current_scheduler != None):
            print(self.current_scheduler)
        else:
            print("No current scheduler")
#___________________________________________________________________________________________________
    def cmd_add_remote(self, arg, opt):
        addr = opt[1].split(':')
        sinfo = SchedulerInfo(opt[0],addr[0],addr[1],"")
        self.available_schedulers[addr[0]+":"+opt[0]] = sinfo
        
        
def main(command, options):
    client = Client()
    inpre = False
    if(command in client.pre_cmd.keys()):
        client.pre_cmd[command](None, options)
        inpre = True
    
    
    if(command in client.commands.keys()):
        client.initialize_socket()
        client.commands[command](None, options)
    else:
        if(not inpre):
            print("Command '%s' not recognized"%command)
        
    client.save_state()

	    
if(__name__ == '__main__'):
       
    #-----------------------------------------------------------------------
    # Get the script's input parameters from the the command line.
    usage = '%prog [command] [options]\n'
    usage +='      valid commands are:\n'
    usage +='      q                  \n'
    usage +='      stop               \n'
    usage +='      n-proc             \n'
    usage +='      rm                 \n'
    usage +='      submit             \n'
    
    parser = OptionParser()
    parser.set_usage(usage)
    
    command = sys.argv[1]
    if(len(sys.argv) >= 3):
        options = list(sys.argv[2:])
    else:
        options = None
        
    (optionss, args) = parser.parse_args() 
    path_here = os.path.dirname(os.path.realpath(__file__))
    main(command, options) 
    
    
    
