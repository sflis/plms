#!/usr/bin/env python

 
import protoScheduler

from optparse import OptionParser
from os.path import expandvars
import os
import sys
import socket
import utils
import pickle
import re
class Client(object):
    def __init__(self):
        path_here = os.path.dirname(os.path.realpath(__file__))
        self.client_name =  "client_"+socket.gethostname()
        self.scheduler_client = None     
        
        
        self.conf_file = open(os.path.join(path_here,"mupys.conf"),'r')
        conf = self.conf_file.readlines()
        self.socket_path = utils.parse(conf,"socket_path")
        self.conf_path = utils.parse(conf,"conf_path")
        self.conf_path_client = utils.parse(conf,"conf_path_client")
        self.state_file = os.path.join(self.conf_path_client, self.client_name+".pkl")
        self.tcp_mode = False
        self.tcp_address = ""
        self.load_state()
        
        self.configure_file = os.path.join(self.conf_path, self.current_scheduler+".conf")
        
        self.current_scheduler = None
        self.available_schedulers = None   
        self.load_state()
        self.pre_cmd =  {'cs'          :self.pre_cmd_change_sch,
                         'as'          :self.pre_cmd_get_available_sch,
                            'tcp-mode'    :self.cmd_mode,
                            'tcp-addr'    :self.cmd_addr}
        
        self.commands = {'q'           :self.cmd_print_queue,
                        'stop'        :self.cmd_stop,
                        'rm'          :self.cmd_rm,
                        'submit'      :self.cmd_submit,
                        'submit-list' :self.cmd_submit_list,
                        'avgload'     :self.cmd_avg_load,
                        'submit-jdf'  :self.cmd_submit_jdf,
                        'which'       :self.cmd_which,
                        'n-proc'      :self.cmd_cn_proc,
                       
                        }
#___________________________________________________________________________________________________
    def load_state(self):
        '''Loads the previus state of the client
        '''
        if(not os.path.isfile(self.state_file)):
            print("No state file found...")
            print("Falling back to hostname scheduler:'%s'"%socket.gethostname())
            self.current_scheduler = "mpls_client_"+socket.gethostname()
            return
        state = pickle.load(open(self.state_file))
        self.current_scheduler = state["current_scheduler"]
        self.tcp_mode = state["tcp_mode"]
        self.tcp_address = state["tcp_address"]
#___________________________________________________________________________________________________
    def save_state(self):
        state = dict()
        state["current_scheduler"] = self.current_scheduler
        state["tcp_mode"] = self.tcp_mode
        state["tcp_address"] = self.tcp_address
        pickle.dump(state, open(self.state_file, 'wb'))
#___________________________________________________________________________________________________
    def initialize_socket(self):
        print(self.tcp_mode)
        print(self.tcp_address)
        if(self.tcp_mode):
            self.scheduler_client = protoScheduler.ProtoSchedulerClient("tcp://"+self.tcp_address,True)
        else:
            self.scheduler_client = protoScheduler.ProtoSchedulerClient(os.path.join("ipc://"+self.socket_path,self.current_scheduler))
        #print(os.path.join("ipc://"+self.socket_path,self.current_scheduler))
#___________________________________________________________________________________________________
    def test_connection(self):
        pass
#___________________________________________________________________________________________________
    def pre_cmd_get_available_sch(self,arg, opt):
        file_list = os.listdir(self.socket_path)
        file_name_format = r'mpls_client_(?P<name>[\w\-.]+)'
        for fn in file_list:
            socket_name = re.match(file_name_format, fn)
            if(socket_name != None):
                print( socket_name.group("name"))
#___________________________________________________________________________________________________
    def cmd_cn_proc(self, arg,opt):
        self.scheduler_client.change_nproc_limit(int(opt[0]))
#___________________________________________________________________________________________________
    def cmd_avg_load(self,arg,opt):
        print(self.scheduler_client.get_avg_load())
#___________________________________________________________________________________________________
    def pre_cmd_change_sch(self,arg, opt):
        self.current_scheduler = "mpls_client_"+opt[0]
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
            return_msg = self.scheduler_client.submit_simple_jobs([" ".join(options)],env = os.environ)
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
        print(self.current_scheduler)
#___________________________________________________________________________________________________
    def cmd_mode(self, arg, opt):
        print(opt)
        if(opt[0] == "on"):
            self.tcp_mode = True
        else:
            self.tcp_mode = False
#___________________________________________________________________________________________________       
    def cmd_addr(self, arg, opt):
            self.tcp_address = opt[0]
        
def main(command, options, socket_name):
    client = Client()
    inpre = False
    if(command in client.pre_cmd.keys()):
        client.pre_cmd[command](None, options)
        inpre = True
    client.initialize_socket()
    if(command in client.commands.keys()):
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
    main(command, options, "ipc://"+ path_here+"/.socket/mpls_client_") 
    
    
    
