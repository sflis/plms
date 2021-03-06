


class Job(object):
    '''
    This class describes a job and contains information to execute the job
    and to keep some statistics about it.
    '''

    #dictionaries to convert between the numerical and string representations of the job status
    statstr_2_id = {"idle":0,"running":10,"held":20,"finished":30,"terminated":40,"removed":50,"failed":60,"start failed":70}
    statid_2_str = {0:"idle",10:"running",20:"held",30:"finished",40:"terminated",50:"removed",60:"failed",70:"start failed"}

    def __init__(self, id, cmd, submit_time, user, log_out='/dev/null', log_err='/dev/null', env = None, name = '', wdir = None, shell = False,cmd_length=100):
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
        self.shell = shell
        self.compress_cmd = cmd_length
        self.exit_status = 0
        self.pid =None
        self.childpids = None
    def update(self,time):
        '''Updates the job statistics with respect to a given time.

            arguments:
            time -- the time to wich the object should be updated to.
        '''
        if(self.status == 'finished' or self.status == 'terminated'):
            time = self.end_time
        elif(self.status == 'idle' or self.status == 'removed'):
            time = 0
        import utils
        def get_time_tuple(time):
            d = int(time/(24*3600))
            h = int((time-d*(24*3600))/3600)
            m = int((time-d*(24*3600)-h*3600)/60)
            s = (time-d*(24*3600)-h*3600-m*60)
            return (d,h,m,s)
        if(len(self.cmd)>0):
            if(self.cmd[-1] == '\n'):
                e = -2
            else:
                e = len(self.cmd)
            if(len(self.cmd)>self.compress_cmd):
                from utils import bcolors as bc
                l = int(self.compress_cmd/2.0)
                self.prop_dict["cmdc"] = self.cmd[:l-3]+bc.bold("...",'\033[38;5;44m')+self.cmd[-l:e]
            else:
                self.prop_dict["cmdc"] = self.cmd[:e+1]
        else:
            self.prop_dict["cmdc"] = self.cmd
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



selection_keys = [k for k in Job.statstr_2_id.keys()]#+['job_id']
#===================================================================================================
class StatusSelector(object):
    def  __init__(self, status_list = list()):
        self.status_list = status_list

    def __call__(self,job):
        ret = True
        if(job.status in self.status_list):
            ret = False

        return ret

    def select(self, job):
        ret = True
        if(job.status in self.status_list):
            ret = False

        return ret

#==============================================================================
def parse_selection_expr(expr, job_list, ids):
    '''Evaluates a python expression to select jobs from the given list of jobs.

        arguments:
        expr     -- python expresion (string)
        job_list -- list of Job objects
        ids      -- a list of job id mapping to the job list.

    '''
    import numpy as np
    for k in selection_keys:
        if(k in expr):
            job_ids = list()
            #Gather job id which are matching
            #the selection key
            for job in job_list:
                if(job.status == k):
                    job_ids.append(job.id)
            #dynamically create a variable matching the selection
            #key
            exec("%s = np.array(job_ids)"%k)
            if(expr==k):
                expr = expr+'=='+expr
            mask = eval(expr)
            return eval("%s[mask]"%k)
    mask = eval(expr)
    ids = np.array(ids)
    return eval("ids[mask]")
#==============================================================================

def job_process2(socket_name, job_description):
    '''This function wraps the job command to be executed.

       It executes the command found in the job_description and redirects all
       output according to the job_description to the appropriate log files.
       It is also responsible to monitor the execution (NOTE:not completely
       implemented yet) and notify the scheduler once the job has finished
       execution.

       arguments:
       socket_name     -- ipc socket path to the scheduler job socket
       job_description -- a Job object
    '''
    import sys
    import os
    import time
    import zmq
    from subprocess import call
    import subprocess
    from multiprocessing import Process

    #import dl
    #libc = dl.open('/lib/libc.so.6')
    #libc.call('prctl', 15,job_description.cmd , 0, 0, 0)

    # redirect standard file descriptors to log files
    sys.stdout.flush()
    sys.stderr.flush()

    so = file(job_description.log_out, 'w')
    se = file(job_description.log_err, 'w')

    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    #setting the enviroment for the job
    if(job_description.env != None):
        os.environ.update(job_description.env)

    if(job_description.wdir != None):
        os.chdir(job_description.wdir)
    else:
        job_description.wdir='/'

    # launch the job
    failed = False
    try:
        if(job_description.shell):
            start_time = time.time()
            return_code = subprocess.call(job_description.cmd, cwd=job_description.wdir, shell=True) #,cwd=job_description.current_dir
        else:
            start_time = time.time()
            return_code = subprocess.call(job_description.cmd.split(), cwd=job_description.wdir, shell=False)
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
    msg += "Status: "+str(return_code)+"\n"
    msg += "Start time: "+str(start_time)+"\n"
    msg += "End time: "+str(finish_time)+"\n"
    msg += "CPU time: "+str(cpu_time)+"\n"

    socket.send(msg)

    msg = socket.recv()
    #good bye

#=====================================================================================================
def job_process(socket_name,start_socket_name, job_description):
    '''This function wraps the job command to be executed.

       It executes the command found in the job_description and redirects all
       output according to the job_description to the appropriate log files.
       It is also responsible to monitor the execution (NOTE:not completely
       implemented yet) and notify the scheduler once the job has finished
       execution.

       arguments:
       socket_name     -- ipc socket path to the scheduler job socket
       job_description -- a Job object
    '''
    import sys
    import os
    import time
    import zmq
    from subprocess import call
    import subprocess
    from multiprocessing import Process

    #import dl
    #libc = dl.open('/lib/libc.so.6')
    #libc.call('prctl', 15,job_description.cmd , 0, 0, 0)

    # redirect standard file descriptors to log files
    sys.stdout.flush()
    sys.stderr.flush()

    so = file(job_description.log_out, 'w')
    se = file(job_description.log_err, 'w')

    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    #setting the enviroment for the job
    if(job_description.env != None):
        os.environ.update(job_description.env)

    if(job_description.wdir != None):
        os.chdir(job_description.wdir)
    else:
        job_description.wdir='/'
    print("Job Wrapper: Start job %d with name %s"%(job_description.id,job_description.name))
    wrapper_pid = os.getpid()
    print("Job Wrapper: %d"%wrapper_pid)

    #Set up sockets to communcate with the scheduler
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc://"+start_socket_name)

    msg = {'id':job_description.id,'pid':wrapper_pid}
    socket.send_pyobj(msg)
    msg = socket.recv_pyobj()
    #socket.close(
    # launch the job
    failed = False
    try:
        if(job_description.shell):
            start_time = time.time()
            return_code = subprocess.call(job_description.cmd, cwd=job_description.wdir, shell=True) #,cwd=job_description.current_dir
        else:
            start_time = time.time()
            return_code = subprocess.call(job_description.cmd.split(), cwd=job_description.wdir, shell=False)
    except:
        failed = True
        return_code = -1
    print("Job Wrapper: Return code: %d"%return_code)

    cpu_time = time.clock()
    finish_time = time.time()
    if(return_code !=0):
        failed = True


    #context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("ipc://"+socket_name)
    #compose message
    msg = str(job_description.id)+"\n"
    msg += "Status: "+str(return_code)+"\n"
    msg += "Start time: "+str(start_time)+"\n"
    msg += "End time: "+str(finish_time)+"\n"
    msg += "CPU time: "+str(cpu_time)+"\n"
    #communicate back the status
    socket.send(msg)

    msg = socket.recv()
    #good bye
