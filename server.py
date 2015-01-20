import os
import sys
from subprocess import call
import subprocess
from multiprocessing import Process
import zmq
import time
import pickle
 
from daemon import Daemon
import atexit
from time import gmtime, strftime
from datetime import datetime
import inspect
import math
import collections

import utils
from utils import job_process
from utils import Job
from utils import Message
from utils import RetMessage
from utils import parse,bcolors

#===================================================================================================
#++++++Class: Server++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#===================================================================================================
class PLMSServer(Daemon):
    ''' The PLMSServer (python local micro scheduler server) is  a simple scheduler class which handles 
        job scheduling on local machines through communications
        whith the a client.
    '''
    
    
    PMLSconf = collections.namedtuple("PMLSconf","tcp_addr, tcp_port, logs_path, n_proc_limit, time_limit, load_state, socket_path")
    
    def __init__(self, 
        scheduler_name         ,#name of the scheduler
        conf_path              ,#path to the configuration file 
        conf = None             #configuration if no configuration file exists yet
        ):
        
        import socket
        #Getting  and setting host name and scheduler name 
        self.host = socket.gethostname()
        self.scheduler_name = scheduler_name
        self.configure_file = os.path.join(conf_path, scheduler_name+".conf")
        
        #initializing log output buffer (a string)
        self.log_output = ""
        self.hold_output = True
        self.version = utils.VERSION
        
        #if no configuration is given and a configuration file is found
        #the configuration is read from the conf file.
        if(os.path.isfile(self.configure_file) and conf == None):
            self.log("Found configure file, loading configuration")
            conf_file = open(self.configure_file,'r')
            conf = conf_file.readlines()
            socket_path = utils.parse(conf, "socket_path")
            logs_path = utils.parse(conf, "logs_path")
            self.n_proc_limit = int(utils.parse(conf, "n_proc_limit"))
            self.proc_time_limit = int(utils.parse(conf, "proc_time_limit"))
            self.tcp_addr = utils.parse(conf, "tcp_address")
            self.tcp_port = utils.parse(conf, "tcp_port")
            if(utils.parse(conf, "load_state") == "True" or utils.parse(conf, "load_state") == "true"):
                init = True
            else:
                init = False
        else:
            if(conf == None):
                print("No previous configuration found or given. Please provide PMLS configuration")
                raise RuntimeError("No previous configuration found or given. Please provide PMLS configuration")
            self.log("No previous configuration. Generating default configuration...")
            self.n_proc_limit = conf.n_proc_limit
            self.proc_time_limit = conf.time_limit
            self.tcp_addr = conf.tcp_addr
            self.tcp_port = conf.tcp_port
            logs_path = conf.logs_path
            socket_path = conf.socket_path
            init = False
            f = open(self.configure_file,'w')
            f.write("#Micro python scheduler configuration file \n")
            f.write("#This file was created automatically when the scheduler with this name was\n")
            f.write("#was started for the first time. This file will be read each time the     \n")
            f.write("#scheduler is started and the settings will be configured from this file. \n")
            f.write("tcp_address:                                                               %s\n"%conf.tcp_addr)
            f.write("tcp_port:                                                                 %s\n"%conf.tcp_port)
            f.write("socket_path:                                                              %s\n"%conf.socket_path)
            f.write("logs_path:                                                                %s\n"%conf.logs_path)
            f.write("n_proc_limit:                                                             %d\n"%conf.n_proc_limit)
            f.write("proc_time_limit:                                                          %d\n"%conf.time_limit)
            f.write("load_state:                                                               %s\n"%conf.load_state)
        #Deamonizing the server   
        Daemon.__init__(self, '/tmp/mpls_'+scheduler_name+'.pid', 
            stdout=conf_path+"/"+scheduler_name+".log", 
            stderr=conf_path+"/"+scheduler_name+".log"
        )
        
        #self.client_socket_name = socket_path+"/pmls_client_"+scheduler_name
        #path to an ipc socket for communications with the running jobs
        self.job_socket_name = socket_path+"/pmls_job_"+scheduler_name
        #path to the file which saves the state of the scheduler server when it 
        #shuts down.
        self.statistics_file = conf_path+"/pmls_stat_"+scheduler_name+".pkl"
        
        self.default_log_path = os.path.join(logs_path,scheduler_name+'/')
        utils.ensure_dir(self.default_log_path)
        
        self.queue = list()
        self.jobs = list()
        self.finished_jobs = list()
        self.job_finish_status = list()
        self.all_jobs = dict()
        self.id_count = 0

        self.quit = False
        self.logging = True
        self.commands = {'SUBMIT_JOBS'  :self.command_SUBMIT_JOBS,
                         'CONFIGURE'    :self.command_CONFIGURE,
                         'REMOVE_JOBS'  :self.command_REMOVE_JOBS,
                         'STOP'         :self.command_STOP,
                         'AVG_LOAD'     :self.command_AVG_LOAD,
                         'PING'         :self.command_PING,
                         'REQUEST_JOBS' :self.command_REQUEST_JOBS,
                         }
        
        if(init):
            self.load_state()
            
        atexit.register(self.finish)
#___________________________________________________________________________________________________
    def load_state(self):
        ''' Loads the state of the scheduler from a previous session. 
      
        '''
        self.log("Loading scheduler state")
        if(not os.path.isfile(self.statistics_file)):
            self.log("No state file found...")
            self.log("Failed to initiate previus scheduler state...")
            return
        self.log("loading previous state from %s"%self.statistics_file)
        state = pickle.load(open(self.statistics_file))
        self.queue = state["queue"]
        self.finished_jobs = state["finished"]
        self.id_count = state["id_count"]
        if("all_jobs" in state.keys()):
            self.all_jobs = state["all_jobs"]
#___________________________________________________________________________________________________
    def init_sockets(self):
        ''' Initializes and binds to sockets for client-server communication (tcp socket) and
            job-server communication (ipc socket). 
        '''
        self.log("Initilizing sockets")
        self.context = zmq.Context()
        self.client_socket = self.context.socket(zmq.REP)
        self.job_socket = self.context.socket(zmq.REP)
        self.log("Binding to client socket: tcp://%s:%s"%(self.tcp_addr,self.tcp_port))
        self.client_socket.bind("tcp://%s:%s"%(self.tcp_addr,self.tcp_port))            
        self.log("Binding to jobb socket: ipc://"+self.job_socket_name)
        self.job_socket.bind("ipc://"+self.job_socket_name)

#___________________________________________________________________________________________________
    def command_SUBMIT_JOBS(self, msg):
        ''' Processes and submits a list of jobs. 
        '''
        n_jobs_added = self.parse_job_submit_list(msg)
        if(n_jobs_added>0):
            return_msg = "SUCCESS\n"
            self.log("Submited %d jobs"%n_jobs_added)
        else:
            return_msg = "FAIL\n"
            self.log("Failed to submit jobs")
        return_msg += str(n_jobs_added)
        return return_msg
#___________________________________________________________________________________________________
    def command_REQUEST_QUEUE(self, msg):
        return_msg = "SUCCESS\n"
        return_msg += "SENDING_LIST\n"
        opt = msg.opt[0]
        fmt_str = msg.opt[1] if(len(msg.opt)>1) else None
        return_msg += self.print_queue(msg.opt[0], fmt_str)
        return return_msg
#___________________________________________________________________________________________________
    def command_CONFIGURE(self, msg):
        ''' Processes configuration commands to the scheduler
            server.
        '''
        if(msg.opt[0] == "NPROC"):
            return_msg = "SUCCESS\n"
            if(msg.msg["n-proc"] != None):
                self.n_proc_limit = int(msg.msg["n-proc"])
            return_msg +="nproc-limit: %d\n"%self.n_proc_limit 
        else:
            return_msg = "FAIL\n"
        return return_msg
#___________________________________________________________________________________________________
    def command_REMOVE_JOBS(self, msg):
        
        if(msg.opt[0] == "ALL"):
            n = self.remove_jobs(None, msg.msg["job_ids"])
            return_msg = "SUCCESS\n"
            return_msg = str(n)
        elif(msg.opt[0] == "LIST"):
            n = self.remove_jobs(msg.msg["job_ids"], msg.user)
            return_msg = "SUCCESS\n"
            return_msg = str(n)
        else:
            return_msg = "FAIL\n"
        return return_msg
#___________________________________________________________________________________________________
    def command_AVG_LOAD(self, msg):
        f = open("/proc/loadavg",'r')
        return f.read()
#___________________________________________________________________________________________________
    def command_STOP(self, msg):
        ''' Processes the stop command message
        '''
        #Stopping the scheduler 'NOW' termiates any running jobs
        if(msg.opt[0] == "NOW"):
            return_msg = "SUCCESS\n"
            n = self.remove_jobs(None, "unkown")
            return_msg = "removed "+str(n)+" jobs.\n"
            return_msg += "Stopping scheduler..."
            self.log("Stopping scheduler now!")      
            self.quit = True
        #Stopping the scheduler 'GENTLE' exits the scheduler when the last running job stops.
        elif(msg.opt[0] == "GENTLE"):
            return_msg = "SUCCESS\n"
            return_msg += "Stopping scheduler..."
            self.quit = True
            self.log("Stopping scheduler gently.")
        else:
            return_msg = "FAIL\n"
        return return_msg
    
#___________________________________________________________________________________________________
    def command_PING(self, msg):
        return_msg = "SUCCESS\n"
        return_msg +=self.scheduler_name+"\n"
        return_msg +=self.host+"\n"
        return return_msg
    
#___________________________________________________________________________________________________
    def command_REQUEST_JOBS(self, msg):        
        '''Returns a message of the requested job or a list of requested jobs. 
        '''
        return_msg = RetMessage(server = self,status = "SUCCES")
        if(msg.opt == None):
            return_msg.msg['jobs'] = self.all_jobs
        elif(msg.opt[0] in self.all_jobs.keys()):
            return_msg.msg['jobs'] = self.all_jobs[msg.opt[0]]
        else:
            return_msg.status = "FAIL"
            return_msg.msg["error"] = "Job id %d not found"%msg.opt[0]
        return return_msg.compose()
#___________________________________________________________________________________________________
    def recv_commands(self):
        '''
            Recieves requests from the client through the ipc socket
        '''
        
        # If no message waits recv will throw an exception
        try:
            message = self.client_socket.recv(flags=zmq.DONTWAIT)
        except:
            return
        
        
        msg = Message()
        msg.decompose(message)
        self.log("Recieved command from client: %s"%msg.cmd)
        if(msg.cmd in self.commands.keys()):
            return_msg = self.commands[msg.cmd](msg)
        else:
            return_msg = "FAIL\n"
        self.log("Returning message to client")    
        self.client_socket.send(return_msg)
        
#___________________________________________________________________________________________________
    def parse_job_submit_list(self, msg):
        
        self.log("Parsing job submit list")
        
        if(msg.opt[0] == 'SIMPLE'):
            for j in msg.msg["cmd_list"]:
                self.add_job(j, msg.user, 
                             self.default_log_path + str(self.id_count)+".out", 
                             self.default_log_path + str(self.id_count)+".err", 
                             env = msg.msg["env"], 
                             current_dir = msg.msg["wdir"],
                             shell = msg.msg["shell"])
            return len(msg.msg["cmd_list"])
        elif(msg.opt[0] == 'SIMPLE_LOG'):
            log_out_path = msg.msg["log_out_path"] 
            log_err_path = msg.msg["log_err_path"]      
            for j in msg_dict["cmd_list"]:
                self.add_job(j, msg.user, 
                             log_out_path + str(self.id_count)+".out", 
                             log_err_path + str(self.id_count)+".err",
                             env = msg.msg["env"], 
                             current_dir = msg.msg["current_dir"],
                             shell = msg.msg["shell"])
            return len(msg.msg["cmd_list"])
        elif(msg.opt[0] == 'JOB_DESCRIPTION'):
            log_out = msg.msg["outlog"] 
            log_err = msg.msg["errlog"]
            cmd = msg.msg["executable"]
            cmd += " "+msg.msg["args"]
            #for arg in msg.msg["args"]:
                #cmd +=" "+arg
            self.add_job(cmd, msg.user, log_out, log_err, env = msg.msg["env"], shell = msg.msg["shell"])
            return 1
        else:
            return -1

#___________________________________________________________________________________________________
    def log(self, msg):
        '''This function provides basic log functionallity for the server'''
        frame,filename,line_number,function_name,lines,index=\
        inspect.getouterframes(inspect.currentframe())[1]
        s = datetime.now().strftime('%Y-%m-%d %H:%M:%S')+" %s:%d in %s :  %s"%(filename,line_number,function_name,msg)
        if(self.hold_output):
            self.log_output += s+"\n"
        else:
            print(s)

#___________________________________________________________________________________________________
    def add_job(self, cmd, user, log_out , log_err, env, current_dir = None, shell = False ):
        job = Job(self.id_count,
                cmd,
                time.localtime(),
                user,
                log_out,
                log_err,
                env,
                '',
                current_dir,
                shell)
        self.queue.append(job)
        self.all_jobs[self.id_count] = job        
        self.id_count +=1
        
#___________________________________________________________________________________________________    
    def remove_jobs(self, ids, user):
        
        n_jobs_removed = 0
        if(ids == None):
            n_jobs_removed = len(self.queue)
            self.queue = list()
            for j in self.jobs:
                j[0].terminate()
                j[1].status = "terminated"
                j[1].end_time = time.time()
                j[1].cpu_time = float("nan")
                self.finished_jobs.append(j[1])
                n_jobs_removed += 1
                self.log("Removed job %d"%j[1].id)
            self.jobs = list()
        else:
            queue = list()
            for j in self.queue:
                if(j.id not in ids):
                    queue.append(j)
                else:
                    j.status = "removed"
                    j.end_time = time.time()
                    j.cpu_time = float("nan")
                    self.finished_jobs.append(j)
                    n_jobs_removed +=1
                    
            self.queue = queue
            
        jobs = list()
        for j in self.jobs:
            if(j[1].id not in ids):
                jobs.append(j)
            else:
                j[0].terminate()
                j[1].status = "terminated"
                j[1].end_time = time.time()
                j[1].cpu_time = float("nan")
                self.finished_jobs.append(j[1])
                #self.jobs.remove(j)
                n_jobs_removed +=1
                self.log("Removed job %d"%j[1].id)
            self.jobs = jobs
        return n_jobs_removed
 #___________________________________________________________________________________________________   
    def pause_jobs(self, ids, user):
        jobs2pause = list()
        for j in list(self.jobs):
            if(j.id in ids):
                pass
              
#___________________________________________________________________________________________________    
    def shuffle_queue(self):
        pass
#___________________________________________________________________________________________________
    def check_jobs(self):
        '''
        Checks if the jobs in the job list are running and starts new jobs from the queue
        when avaiable job slots open.
        '''

        job_message = ""
        while(True):
            # If no message waits recv will throw an exception
            try:
                message = self.job_socket.recv(flags=zmq.DONTWAIT)
            except:
                break
                
            #send back an 'OK'
            self.job_socket.send("OK")
            lines = message.splitlines()
            #print(lines)
            job_id = int(lines[0])
            self.job_finish_status += [(job_id,lines[1:])]
                    
        for j in self.jobs:
            for s in self.job_finish_status:
                if(not j[0].is_alive() and j[1].id == s[0]):
                    
                    #print(self.job_finish_status)
                    #print(j[1].end_time)
                    j[1].status = "finished"
                    j[1].end_time = float(parse(s[1], "End time"))
                    j[1].start_time = float(parse(s[1], "Start time"))
                    j[1].cpu_time = float(parse(s[1], "CPU time"))
                    self.finished_jobs.append(j[1])
                    self.jobs.remove(j)
                    self.job_finish_status.remove(s)
                    
        #If the length of the job list is shorter than the n_proc limit and there are more jobs in the
        #queue, new jobs should be added to the job list and be started
        while(len(self.jobs) < self.n_proc_limit and len(self.queue) > 0 and not self.quit):
            queued_job = self.queue.pop(0)
            queued_job.status = "running"
            queued_job.start_time = time.time()
            self.jobs.append( (Process(target=job_process, args=(self.job_socket_name, queued_job)),queued_job))
            self.jobs[-1][0].start()
#___________________________________________________________________________________________________
    def run(self):
        '''The run method is where the main loop is
            
        '''
        self.log("Starting scheduler")
        self.hold_output = False
        print(self.log_output)
        self.init_sockets()
        
        while(not self.quit or len(self.jobs)>0):
            time.sleep(0.3)

            self.recv_commands()
            
            self.check_jobs()
            sys.stdout.flush()
            sys.stderr.flush()      
#___________________________________________________________________________________________________        
    def finish(self):
        '''The finish method is called at exit.
        This method should clean and save the current state of the scheduler. 
        
        '''
        self.log("Saving state")
        state = dict()
        state["queue"] = self.queue
        state["finished"] = self.finished_jobs
        state["id_count"] = self.id_count
        state["all_jobs"] = self.all_jobs  
        pickle.dump(state, open(self.statistics_file, 'wb'))
        
