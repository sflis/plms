#!/usr/bin/env python

from plms import server,utils

from optparse import OptionParser
from os.path import expandvars
import os
import sys
import socket


def main(scheduler_name = None, daemonize = False, new = False, port='5555'):
    path_here = os.path.dirname(os.path.realpath(__file__))
    conf_file = open(os.path.join(path_here,"plms.conf"),'r')
    conf = conf_file.readlines()
    
    socket_path = utils.parse(conf,"socket_path")
    logs_path = utils.parse(conf,"logs_path")
    conf_path = utils.parse(conf,"conf_path")

    if(scheduler_name == None):
        scheduler_name = socket.gethostname()
        
    if(new):
        configuration =  server.PLMSServer.PMLSconf(socket_path = socket_path,
                                logs_path = logs_path,
                                n_proc_limit = 2,
                                time_limit = -1,
                                load_state = True,
                                tcp_addr = "127.0.0.1",
                                tcp_port = port)
    else:
        configuration = None
        
    pscheduler = server.PLMSServer(scheduler_name, conf_path, configuration)
    
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
        help    = "Name of the scheduler. Is used to distinguish different schedulers running simultaneously on the same host."
    )
    
    parser.add_option("-d", "--daemonize",
        action  = "store_true",
        dest    = "daemonize",
        help    = "If the scheduler should be started as a daemon."
    )
    
    parser.add_option("-n", "--new",
        action  = "store_true",
        dest    = "new",
        help    = "If a scheduler is started for the first time then configuration can be provided by the "+\
                  "command line or use defaults. Using -n also overrides the preavious configuration with the one from the command line."
    )
    
    parser.add_option("-p", "--port",
        action  = "store",
        type    = "string",
        default = '5555',
        dest    = "port",
        help    = "This is the port which the scheduler should listen to."
    )
    
    
    (options, args) = parser.parse_args() 
    
    main(options.scheduler_name, options.daemonize, options.new, options.port) 
    
    