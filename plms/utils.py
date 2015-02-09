import os
import sys
import pickle
from time import gmtime, strftime
from datetime import datetime
import time


VERSION = 0.2
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
    index = 0
    for o in opt_list:
        if('-' in o[0]):
            if(opt in o):
                return True
        index +=1
    return False 
#===================================================================================================       
def parse_arg(opt_list,opt):
    ret = False
    index = 0
    for o in opt_list:
        if('-' in o[0]):
            if(opt in o):
                return True,index
        index +=1
    return False, 0

    
    
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
#=====================================================================================================       
def get_object_prop(obj):
    return [a for a in dir(obj) if not a.startswith('__') and not callable(getattr(obj,a))]
#=====================================================================================================        
class bcolors:
    HEADER = '\033[35m\033[95m'
    OKBLUE = '\033[34m\033[94m'
    OKGREEN = '\033[32m\033[92m'
    WARNING = '\033[33m\033[93m'
    FAIL = '\033[1;31m'
    RED = '\033[31m\033[91m'
    CYAN = '\033[36m\033[96m'
    LIGHT_BLUE = '\033[94m'
    LIGHT_MAGENTA = '\033[95m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    @staticmethod
    def err(str):
        return bcolors.FAIL+bcolors.BOLD+str+bcolors.ENDC
    @staticmethod
    def gen(str,opt):
        return opt+str+bcolors.ENDC
    @staticmethod
    def bold(str,opt=''):
        return opt+bcolors.BOLD+str+bcolors.ENDC
    @staticmethod
    def head(str):
        return bcolors.HEADER+str+bcolors.ENDC
    
def queued(str):
    return bcolors.WARNING+str+bcolors.ENDC
def running(str):
    return bcolors.OKBLUE+str+bcolors.ENDC
def finished(str):
    return bcolors.OKGREEN+str+bcolors.ENDC
def terminated(str):
    return bcolors.RED+str+bcolors.ENDC

colors = {'idle':queued, 'running':running, 'finished':finished, 'terminated':terminated, 'removed':terminated}