import os
import sys
from subprocess import call
import subprocess
from multiprocessing import Process
import zmq
import time
import pickle
import utils 
from daemon import Daemon
import atexit
from time import gmtime, strftime
from datetime import datetime
import inspect
import math

class Job(object):
    '''
    This class describes a job and contains information to execute the job 
    and to keep some statistics about it.
    '''
    def __init__(self, id, cmd, submit_time, user, log_out='/dev/null', log_err='/dev/null', env = None):
	self.id=id
	self.cmd = cmd
	self.status = "idle"
	self.submit_time = submit_time
	self.start_time = 0
	self.end_time = 0
	self.cpu_time = 0
	self.user = user
	self.log_out=log_out
	self.log_err=log_err
	self.env = env
	
	
class Message(object):
    def __init__(self, command = None, options = None, user = None):
        self.msg = dict()
        self.msg['command'] = command
        if(isinstance(options,list) or options == None): 
	    self.msg['options'] = options
	else:
	    self.msg['options'] = [options]
        self.msg['user'] = user
        self.cmd = command
        self.opt = options
        self.user = user
        self.raw_msg = None
    def compose(self):
	self.raw_msg = pickle.dumps(self.msg) 
	return self.raw_msg
    
    def check_sanity(self, add_keys = None):
	std_keys = ['command','options','user']
	if(add_keys != None):
	    std_keys.append(add_keys)
	for k in std_keys:
	    if(k not in self.msg.keys()):
		raise ValueError("Key '%s' not present in message."%k)
	    
    def decompose(self, msg):
	self.raw_msg = msg
	self.msg = pickle.loads(msg)
	self.check_sanity()
	self.cmd = self.msg['command']
	self.opt = self.msg['options']
	self.user = self.msg['user']
	return self.msg

class RetMessage(object):
    def __init__(self, name = None, host = None , status = None):
	self.msg = dict()
	self.name = name
	self.host = host
	self.status = status
	self.composed = None
    def composte(self):
	self.composed  = pickle.dumps(self)
    def decompose(self, msg):
	self = pickle.loads(msg)
    
#===================================================================================================
#++++++Class: ProtoScheduler++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#===================================================================================================
class ProtoScheduler(Daemon):
    '''ProtoScheduler a simple scheduler class which handles job scheduling on local machines through communications
    whith the ProtoSchedulerClient class.
    '''
    def __init__(self, 
        scheduler_name         ,
        conf_path              ,
        socket_path            ,
        logs_path              ,
        n_proc_limit       =  4, 
        time_limit         = -1,
        init               = False,
        tcp                = None):
        
        
        self.scheduler_name = scheduler_name
        self.configure_file = os.path.join(conf_path, scheduler_name+".conf")
        self.log_output = ""
        self.hold_output = True
        self.tcp_mode = False
        self.tcp_address = ""
        if(tcp != None):
            self.tcp_mode = True
            self.tcp_address = tcp
        if(os.path.isfile(self.configure_file)):
            self.log("Found configure file, loading configuration")
            conf_file = open(self.configure_file,'r')
            conf = conf_file.readlines()
            socket_path = utils.parse(conf, "socket_path")
            logs_path = utils.parse(conf, "logs_path")
            self.n_proc_limit = int(utils.parse(conf, "n_proc_limit"))
            self.proc_time_limit = int(utils.parse(conf, "proc_time_limit"))
            if(utils.parse(conf, "load_state") == "true"):
                init = True
            else:
                init = False
        else:
            self.log("No previous configuration. Generating default configuration...")
            self.n_proc_limit = n_proc_limit
            self.proc_time_limit = time_limit
            init = False
            f = open(self.configure_file,'w')
            f.write("#Micro python scheduler configuration file \n")
            f.write("#This file was created automatically when the scheduler with this name was\n")
            f.write("#was started for the first time. This file will be read each time the     \n")
            f.write("#scheduler is started and the settings will be configured from this file. \n")
            f.write("socket_path:                                                              %s\n"%socket_path)
            f.write("logs_path:                                                                %s\n"%logs_path)
            f.write("n_proc_limit:                                                             %d\n"%n_proc_limit)
            f.write("proc_time_limit:                                                          %d\n"%time_limit)
            f.write("load_state:                                                               %s\n"%"true")

        Daemon.__init__(self, '/tmp/mpls_'+scheduler_name+'.pid', 
            stdout=conf_path+"/"+scheduler_name+".log", 
            stderr=conf_path+"/"+scheduler_name+".log"
        )
        
        self.client_socket_name = socket_path+"/mpls_client_"+scheduler_name
        self.job_socket_name = socket_path+"/mpls_job_"+scheduler_name
        self.statistics_file = conf_path+"/mpls_stat_"+scheduler_name+".pkl"
        
        self.default_log_path = os.path.join(logs_path,scheduler_name+'/')
        utils.ensure_dir(self.default_log_path)
        
        self.queue = list()
        self.jobs = list()
        self.finished_jobs = list()
        self.job_finish_status = list()
        self.id_count = 0

        self.quit = False
        self.logging = True
        self.commands = {'SUBMIT_JOBS'  :self.command_SUBMIT_JOBS,
                         'REQUEST_QUEUE':self.command_REQUEST_QUEUE,
                         'CONFIGURE'    :self.command_CONFIGURE,
                         'REMOVE_JOBS'  :self.command_REMOVE_JOBS,
                         'STOP'         :self.command_STOP,
                         'AVG_LOAD'     :self.command_AVG_LOAD
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
        self.tcp_mode = state["tcp_mode"]
#___________________________________________________________________________________________________
    def init_sockets(self):
        self.log("Initilizing sockets")
        self.context = zmq.Context()
        self.client_socket = self.context.socket(zmq.REP)
        self.job_socket = self.context.socket(zmq.REP)
        
        if(self.tcp_mode):
            self.log("Binding to socket: %s"%"tcp://"+self.tcp_address)
            self.client_socket.bind("tcp://"+self.tcp_address)
            
        else:
            self.log("Binding to socket: %s"%"ipc://"+self.client_socket_name)
            self.client_socket.bind("ipc://"+self.client_socket_name)
        print(self.job_socket_name)
        self.job_socket.bind("ipc://"+self.job_socket_name)

#___________________________________________________________________________________________________
    def command_SUBMIT_JOBS(self, msg):
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
        return_msg += self.print_queue(msg.opt[0])
        return return_msg
#___________________________________________________________________________________________________
    def command_CONFIGURE(self, msg):
        if(msg.opt[0] == "NPROC"):
            return_msg = "SUCCESS\n"
            self.n_proc_limit = int(msg.msg["n-proc"])
        else:
            return_msg = "FAIL\n"
        return return_msg
#___________________________________________________________________________________________________
    def command_REMOVE_JOBS(self, msg):
        if(msg.opt[0] == "ALL"):
            n = self.remove_jobs(None, msg.msg["job_ids"])
            return_msg = "SUCCESS\n"
            return_msg = str(n)
        elif(msg.options[0] == "LIST"):
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
        
        if(msg.opt[0] == "NOW"):
            return_msg = "SUCCESS\n"
            n = self.remove_jobs(None, "unkown")
            return_msg = "removed "+str(n)+" jobs.\n"
            return_msg += "Stopping scheduler..."
            self.log("Stopping scheduler now!")      
            self.quit = True
        elif(msg.opt[0] == "GENTLE"):
            return_msg = "SUCCESS\n"
            return_msg += "Stopping scheduler..."
            self.quit = True
            self.log("Stopping scheduler gently.")
        else:
            return_msg = "FAIL\n"
        return return_msg
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
        
        self.log("Recieved command from client")
        msg = Message()
        msg.decompose(message)

        if(msg.cmd in self.commands.keys()):
            return_msg = self.commands[msg.cmd](msg)
        else:
            return_msg = "FAIL\n"
            return_msg += lines
        self.log("Returning message to client")    
        self.client_socket.send(return_msg)
        
#___________________________________________________________________________________________________
    def parse_job_submit_list(self, msg):
        self.log("Parsing job submit list")
        if(msg.opt[0] == 'SIMPLE'):
            for j in msg.msg["cmd_list"]:
                self.add_job(j, msg.user, self.default_log_path + str(self.id_count)+".out", self.default_log_path+str(self.id_count)+".err", env = msg.msg["env"])
            return len(msg.msg["cmd_list"])
        elif(msg.opt[0] == 'SIMPLE_LOG'):
            log_out_path = msg.msg["log_out_path"] 
            log_err_path = msg.msg["log_err_path"]	    
            for j in msg_dict["cmd_list"]:
                self.add_job(j, msg.user, log_out_path + str(self.id_count)+".out", log_err_path + str(self.id_count)+".err",env = msg.msg["env"])
            return len(msg.msg["cmd_list"])
        elif(msg.opt[0] == 'JOB_DESCRIPTION'):
            log_out = msg.msg["outlog"] 
            log_err = msg.msg["errlog"]
            cmd = msg.msg["executable"]
            for arg in msg.msg["args"]:
                cmd +=" "+arg
            self.add_job(cmd, msg.user, log_out, log_err, env = msg.msg["env"])
            return 1
        else:
            return -1

#___________________________________________________________________________________________________
    def log(self, msg):
        
        frame,filename,line_number,function_name,lines,index=\
        inspect.getouterframes(inspect.currentframe())[1]
        s = datetime.now().strftime('%Y-%m-%d %H:%M:%S')+" %s:%d in %s :  %s"%(filename,line_number,function_name,msg)
        if(self.hold_output):
            self.log_output += s+"\n"
        else:
            print(s)

#___________________________________________________________________________________________________
    def add_job(self, cmd, user, log_out , log_err, env ):

        self.queue.append(Job(self.id_count,
                cmd,
                time.localtime(),
                user,
                log_out,
                log_err,
                env))
                
        self.id_count +=1
        
#___________________________________________________________________________________________________
    def print_queue(self, opt):
        self.log("Printing queue with opt %s"%opt)
        now = time.time()
        tot_running_time = 0
        printed_queue="    ID     STATUS       SUBMITED            RUNNING TIME         CMD\n"
        if(opt.find("R") >= 0):
            for j in self.jobs:
                run_time = now - j[1].start_time
                d = int(run_time/(24*3600))
                h = int((run_time-d*(24*3600))/3600)
                m = int((run_time-d*(24*3600)-h*3600)/60)
                s = (run_time-d*(24*3600)-h*3600-m*60)
                running_time_str = "%02dd  %02d:%02d:%05.2fh"%(d,h,m,s)
                printed_queue += "  %06d:%10s: %s : %s : %s\n"%(j[1].id, j[1].status, time.strftime("%Y-%m-%d %H:%M:%S",j[1].submit_time), running_time_str, j[1].cmd[:15])
                tot_running_time +=run_time  
        if(opt.find("Q") >= 0):
            for j in self.queue:
                printed_queue += "  %06d:%10s: %s :                   : %s\n"%(j.id,j.status, time.strftime("%Y-%m-%d %H:%M:%S",j.submit_time),j.cmd[:15])
        if(opt.find("F") >= 0):
            for j in self.finished_jobs:
                
                if(opt.find("U")>=0):
                    if(math.isnan(j.cpu_time)):
                        run_time = 0
                    else:
                        run_time = j.cpu_time
                else:
                    run_time = j.end_time - j.start_time
                
                d = int(run_time/(24*3600))
                h = int((run_time-d*(24*3600))/3600)
                m = int((run_time-d*(24*3600)-h*3600)/60)
                s = (run_time-d*(24*3600)-h*3600-m*60)
                running_time_str = "%02dd  %02d:%02d:%05.2fh"%(d,h,m,s)
                printed_queue += "  %06d:%10s: %s : %s : %s\n"%(j.id, j.status, time.strftime("%Y-%m-%d %H:%M:%S",j.submit_time), running_time_str, j.cmd[:15])
                tot_running_time +=run_time
            
            
        d = int(tot_running_time/(24*3600))
        h = int((tot_running_time-d*(24*3600))/3600)
        m = int((tot_running_time-d*(24*3600)-h*3600)/60)
        s = (tot_running_time-d*(24*3600)-h*3600-m*60)
        running_time_str = "%02dd  %02d:%02d:%05.2fh"%(d,h,m,s)
        printed_queue += "idle jobs: %d, running jobs: %d, total run time: %s\n"%(len(self.queue),len(self.jobs),running_time_str)
        printed_queue += "On: %s"%self.scheduler_name
        return printed_queue

#___________________________________________________________________________________________________	
    def remove_jobs(self, ids, user):
        
        n_jobs_removed = 0
        if(ids == None):
            n_jobs_removed = len(self.queue)
            self.queue = list()
            for j in self.jobs:
                j[0].terminate()
                j[1].status = "Terminated"
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
                    n_jobs_removed +=1
            self.queue = queue
            
        jobs = list()
        for j in self.jobs:
            if(j[1].id not in ids):
                jobs.append(j)
            else:
                j[0].terminate()
                j[1].status = "Terminated"
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
        #fail_recv = False
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
                    j[1].end_time = parse(s[1], "End time")
                    j[1].start_time = parse(s[1], "Start time")
                    j[1].cpu_time = parse(s[1], "CPU time")
                    self.finished_jobs.append(j[1])
                    self.jobs.remove(j)
                    self.job_finish_status.remove(s)
                    
        #If the length of the job list is shorter than the n_proc limit and there are more jobs in the
        #queue new jobs should be added to the job list and be started
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
        stat = dict()
        stat["queue"] = self.queue
        stat["finished"] = self.finished_jobs
        stat["id_count"] = self.id_count
        stat["tcp_mode"] = self.tcp_mode
        pickle.dump(stat, open(self.statistics_file, 'wb'))
        
        
    
#===================================================================================================
#++++++Class: ProtoSchedulerClient++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#===================================================================================================
from zmq import ssh
class ProtoSchedulerClient(object):
    
    def __init__(self, socket_name, tcp = False):
        self.socket_name = socket_name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        if(tcp):
            self.tunnel = ssh.tunnel_connection(self.socket, "tcp://127.0.0.1:5555","lap-flis.physto.se")
            self.socket.setsockopt(zmq.LINGER, 0)
        else:
            self.socket.connect(self.socket_name)
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
    def submit_simple_jobs(self, cmd_list, outlog_path  = None, errlog_path = None, user = "Unknown", env = None):
        if(outlog_path == None or errlog_path == None):
            msg = Message('SUBMIT_JOBS', 'SIMPLE', user)
            msg.msg["cmd_list"] = cmd_list
            msg.msg["env"] = env
        else:
            msg = Message( 'SUBMIT_JOBS', 'SIMPLE_LOG', user)
            msg.msg["cmd_list"] = cmd_list
            msg.msg["env"] = env
            msg.msg["outlog_path"] = outlog_path 
            msg.msg["errlog_path"] = errlog_path
                
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
    def remove_jobs(self, ids = None, user = "Unknown"):	
        
        msg = 'REMOVE_JOBS\n'
        if(ids == None):
            opt = 'ALL'
        else:
            opt = 'LIST'
        msg = Message('REMOVE_JOBS', opt, user)
        msg["job_ids"] = ids
        return self.send_msg(msg.compose())

#===================================================================================================	
def job_process(socket_name, job_description):
    '''This function wraps the job. It executes the command
    found in the job_description and redirects all output according 
    to the job_description to the appropriate log files.
    It is also responsible to monitor the execution (NOTE:not completely implemented yet)
    and notify the scheduler once the job has finished execution. 
    '''
    
    # redirect standard file descriptors to log files
    sys.stdout.flush()
    sys.stderr.flush()
    
    so = file(job_description.log_out, 'w')
    se = file(job_description.log_err, 'w')
    
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    print("Entering Job Wrapper")
    
#    print(job_description.env)
    #setting the enviroment for the job
    if(job_description.env != None):
        #import os
        os.environ.update(job_description.env)

    if(job_description.env == None):
        job_description.env = os.environ
    # launch the job
    failed = False
    try:
        start_time = time.time()
        return_code = subprocess.call(job_description.cmd, shell=True)

    except:
        failed = True
        return_code = -1
    print(return_code)
    cpu_time = time.clock()
    finish_time = time.time()
    if(return_code !=0):
        failed = True
    
    
    
    #communicate back the status
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc://"+socket_name)
    #compose message
    msg = str(job_description.id)+"\n"
    if(failed):
        status = 0
    else:
        status = 1
    msg += "Status: "+str(status)+"\n"
    msg += "Start time: "+str(start_time)+"\n"
    msg += "End time: "+str(finish_time)+"\n"
    msg += "CPU time: "+str(cpu_time)+"\n"
    
    socket.send(msg)
    
    msg = socket.recv()
    #good bye
    
#===================================================================================================	   
#Simple stupid parser....
def parse(string_list, parse_string, n = 0):

    for line in string_list:
        line = line.split(':')

        if(line[0] == parse_string):
            el = line[1].split()           
            return float(el[n])
            
