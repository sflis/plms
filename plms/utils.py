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
    ''' A simple key-value parser.

        Finds the line in which a parse string is present and returns the associated
        value. Example of a default format steering file:
        key1:   value1
        key2:   value2
        arguments:
        string_list -- the list of strings to be searched for the key
        parse_string -- the key word to be found
        n -- if not the first value after keyword us
    '''
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

    '''Not really a parser. Checks if an option 'opt' key is in the opt_list.

        The assumption is that options keys are prefixed with a '-'.
    '''
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
#===================================================================================================
def extract_job_id(arg_list, jobs):
    from job import parse_selection_expr
    ids = []
    for a in arg_list:
        #If argument is a number interpret as job id
        if(is_integer(a)):
            ids.append(int(a))
        elif('-' in a):
            continue
        else:
            ids += list(parse_selection_expr(a,[v for k,v in jobs.items()],jobs.keys()))
    return ids

#===================================================================================================
def is_integer(s):
    '''Checks if a string can be interpreted as an integer.

    '''
    try:
        int(s)
        return True
    except ValueError:
        return False

#=====================================================================================================
def get_object_prop(obj):
    return [a for a in dir(obj) if not a.startswith('__') and not callable(getattr(obj,a))]
#=====================================================================================================
class bcolors:
    '''A small class that contains definitions of terminal colors.
    '''
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
