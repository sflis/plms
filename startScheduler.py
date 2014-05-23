#!/usr/bin/env python

import protoScheduler

from optparse import OptionParser
from os.path import expandvars
import os
import sys
import socket
import utils

def main(scheduler_name = None, daemonize = False):
    path_here = os.path.dirname(os.path.realpath(__file__))
    conf_file = open(os.path.join(path_here,"mupys.conf"),'r')
    conf = conf_file.readlines()
    
    socket_path = utils.parse(conf,"socket_path")
    logs_path = utils.parse(conf,"logs_path")
    conf_path = utils.parse(conf,"conf_path")

    if(scheduler_name == None):
        scheduler_name = socket.gethostname()
        
        
        pscheduler = protoScheduler.ProtoScheduler(scheduler_name, conf_path, 
        socket_path, 
        logs_path,
        2, 
        init = True)
    
    pscheduler.start(daemonize)  

if(__name__ == '__main__'):
       
    #-----------------------------------------------------------------------
    # Get the script's input parameters from the the command line.
    usage = '%prog [options]'
    parser = OptionParser()
    parser.set_usage(usage)
    
    parser.add_option("-s", "--scheduler-name",
        action  = "store",
        type    = "string",
        default = None,
        dest    = "scheduler_name",
        help    = "Name of the scheduler. Is used to distinguish different schedulers running simultaneously on the same machine."
    )
    
    parser.add_option("-d", "--daemonize",
        action  = "store_true",
        dest    = "daemonize",
        help    = "If the scheduler should be started as a daemon."
    )
    
    (options, args) = parser.parse_args() 
    
    main(options.scheduler_name, options.daemonize) 
    
    
    