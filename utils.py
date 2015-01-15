import os
import sys
import pickle
from time import gmtime, strftime
from datetime import datetime
import time
import zmq
from subprocess import call
import subprocess
from multiprocessing import Process

#function to create directory that check if the directory alread have been created.
def ensure_dir(f):
    d = os.path.dirname(f)
    #print(d)
    if not os.path.exists(d):
        os.makedirs(d)
#===================================================================================================	   
#Simple stupid parser....
def parse(string_list, parse_string, n = 0, separator=':',complete_line = False):

    for line in string_list:
        line = line.split(separator, 1)

        if(line[0] == parse_string):
            if(complete_line):
                return line[1]
            else:
                el = line[1].split()
                return el[n]

#===================================================================================================       
def parse_opt(opt_list,opt):
    ret = False
    for o in opt_list:
        if('-' in o):
            if(opt in o):
                return True
    return False
#=====================================================================================================

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
    
    #print(job_description.env)
    #setting the enviroment for the job
    if(job_description.env != None):
        #import os
        os.environ.update(job_description.env)

    if(job_description.env == None):
        job_description.env = os.environ
        
    #if(job_description.current_dir != None):
        #print(job_description.current_dir)
    os.chdir(job_description.current_dir) 
    
    # launch the job
    failed = False
    try:
        start_time = time.time()
        return_code = subprocess.call(job_description.cmd, cwd=job_description.wdir, shell=True) #,cwd=job_description.current_dir
    except:
        failed = True
        return_code = -1
    print("Return code: %d"%return_code)

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
    
#=====================================================================================================    
class Job(object):
    '''
    This class describes a job and contains information to execute the job 
    and to keep some statistics about it.
    '''
    
    statstr_2_id = {"idle":0,"running":10,"held":20,"finished":30,"Terminated":40}
    statid_2_str = {0:"idle",10:"running",20:"held",30:"finished",40:"Terminated"}
    
    def __init__(self, id, cmd, submit_time, user, log_out='/dev/null', log_err='/dev/null', env = None, name = '', wdir = None):
        self.id = id
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
        self.prop_dict = dict()
        self.name = name
        self.wdir = wdir
    def update(self,time):
        def get_time_tuple(time):
            d = int(time/(24*3600))
            h = int((time-d*(24*3600))/3600)
            m = int((time-d*(24*3600)-h*3600)/60)
            s = (time-d*(24*3600)-h*3600-m*60)
            return (d,h,m,s)    
        self.prop_dict["cmd"] = self.cmd
        self.prop_dict["status"] = self.status
        self.prop_dict["status_id"] = Job.statstr_2_id[self.status]
        self.prop_dict["user"] = self.user
        self.prop_dict["id"] = self.id
        (d,h,m,s) = get_time_tuple(time - self.start_time)
        self.prop_dict["run_time"] = time - self.start_time
        self.prop_dict["run_time_days"] = d
        self.prop_dict["run_time_hours"] = h
        self.prop_dict["run_time_minutes"] = m
        self.prop_dict["run_time_seconds"] = s
     
    def formated_output(self, format_str):
        return  format_str%self.prop_dict
    
    
#=====================================================================================================        
class Message(object):
    def __init__(self, command = None, options = None,  user = None, host = None):
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
        self.host = host
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
    
#=====================================================================================================
class RetMessage(object):
    def __init__(self, server = None, status = None):
        self.msg = dict()
        if(server != None):
            self.scheduler_name = server.scheduler_name
            self.host = server.host
        self.status = status
        self.composed = None
    def compose(self):
        self.composed  = pickle.dumps(self)
        return self.composed
    def decompose(self, msg):
        self = pickle.loads(msg)
        
        