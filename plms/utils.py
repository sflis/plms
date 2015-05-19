import os
import sys
import pickle
from time import gmtime, strftime
from datetime import datetime
import time


VERSION_MAJOR = 0
VERSION_MINOR = 1
VERSION_PATCH = 6

#function to create directory that check if the directory alread have been created.
def ensure_dir(f):
    d = os.path.dirname(f)
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
    @staticmethod
    def warn(str):
        return bcolors.WARNING+bcolors.BOLD+str+bcolors.ENDC

def queued(str):
    return bcolors.WARNING+str+bcolors.ENDC
def running(str):
    return bcolors.OKBLUE+str+bcolors.ENDC
def finished(str):
    return bcolors.OKGREEN+str+bcolors.ENDC
def terminated(str):
    return bcolors.RED+str+bcolors.ENDC
def failed(str):
    return bcolors.bold(str,bcolors.RED)

colors = {'idle':queued, 'running':running, 'finished':finished, 'terminated':terminated, 'removed':terminated,'failed':failed}
