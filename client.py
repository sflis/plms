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
        self.pre_cmd =  {'cs'          :(self.pre_cmd_change_sch,'change scheduler'),
                         'as'          :(self.pre_cmd_get_available_sch,'list available scheduers'),
                         'add-remote'  :(self.cmd_add_remote,'add remote scheduler'),
                         'which'       :(self.cmd_which,'which scheduler is used'),
                         'avgloadl'    :(self.cmd_avgloadl,'list the average load on the hosts of the available schedulers'),
                         }
        
        
        self.commands = {'q'          :(self.cmd_print_queue,'queue'),
                        'stop'        :(self.cmd_stop,'stop scheduler'),
                        'rm'          :(self.cmd_rm,'remove jobs'),
                        'submit'      :(self.cmd_submit,''),
                        'submit-list' :(self.cmd_submit_list,''),
                        'avgload'     :(self.cmd_avg_load,'returns the average load on the host from the current scheduler'),
                        'submit-jdf'  :(self.cmd_submit_jdf,''),
                        
                        'n-proc'      :(self.cmd_cn_proc,''),
                        'ping'        :(self.cmd_ping,'')
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
        for k in self.available_schedulers.keys():
            print("%30s | %s"%(k,self.available_schedulers[k]))
#___________________________________________________________________________________________________
    def cmd_cn_proc(self, arg,opt):
        if(opt == None ):
            limit = None
        else:
            limit = int(opt[0])
        print(self.scheduler_client.change_nproc_limit(limit))
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
            queue = self.scheduler_client.request_job_queue(opt)
        print(queue.decode('string_escape'))
        #for l in queue.splitlines()[2:]:
            #print(l)
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
            print("Error: Must pass a command to submit")
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
            print("Error: no file provided")
        else:
            joblist = open(opt[0],'r').readlines()
            self.scheduler_client.submit_simple_jobs(joblist, env = os.environ)	
            
#___________________________________________________________________________________________________
    def cmd_ping(self,arg, opt):
        (name,host,dt) = self.scheduler_client.ping()
        print("Ping: %s,  %fms"%(self.current_scheduler.tcp_addr,dt*1e3))
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
        
#___________________________________________________________________________________________________        
    def cmd_avgloadl(self,arg, opt):
        print("Not implemented yet")
        return
        import copy
        for k in self.available_schedulers.keys():
            print("%30s | %s"%(k,self.available_schedulers[k]))
            temp_client = copy.copy(self)
            temp_client.pre_cmd_change_sch([],[k])
            try:
                temp_client.initialize_socket()
            except:
                
                print("timed out")
                import sys
                sys.stdout.flush()
                del temp_client
                continue
            (name, host, dt) = temp_client.scheduler_client.ping()
            print("Ping: %s,  %fms"%(self.current_scheduler.tcp_addr,dt*1e3))
            print("Host: %s, Name: %s"%(host,name))
            temp_client.cmd_avg_load([],[])
#___________________________________________________________________________________________________        
    def print_help(self):
        usage = '%prog [command] [options]\n'
        usage +='      valid commands are:\n'
        for cmd_key in self.pre_cmd.keys():
                usage +="     %+15s       %-15s\n"%(cmd_key,self.pre_cmd[cmd_key][1])
        for cmd_key in self.commands.keys():
                usage +="     %+15s       %-15s\n"%(cmd_key,self.commands[cmd_key][1])
        return usage              
def main(command, options, client):
    
    inpre = False
    if(command in client.pre_cmd.keys()):
        client.pre_cmd[command][0](None, options)
        inpre = True
    
    
    if(command in client.commands.keys()):
        client.initialize_socket()
        client.commands[command][0](None, options)
    else:
        if(not inpre):
            print("Command '%s' not recognized"%command)
            print(client.print_help())
    client.save_state()

	    
if(__name__ == '__main__'):
       
    #-----------------------------------------------------------------------
    # Get the script's input parameters from the the command line.
    client = Client()
    usage =  client.print_help()
    parser = OptionParser()
    parser.set_usage(usage)
    
    command = sys.argv[1]
    if(len(sys.argv) >= 3):
        options = list(sys.argv[2:])
    else:
        options = None
        
    (optionss, args) = parser.parse_args() 
    path_here = os.path.dirname(os.path.realpath(__file__))
    main(command, options,client) 
    
    
    
