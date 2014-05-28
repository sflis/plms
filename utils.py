import os


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
        line = line.split(separator)

        if(line[0] == parse_string):
            if(complete_line):
                return line[1].split()
            else:
                el = line[1].split()
                return el[n]

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
    
#=====================================================================================================    
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
        
#=====================================================================================================        
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
    
#=====================================================================================================
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