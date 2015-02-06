#!/usr/bin/env python
from optparse import OptionParser
from os.path import expandvars
import os
import sys
from plms import utils


def create_dir(f):
    utils.ensure_dir(f)
    print("created directory '%s'"%f)

def create_default_conf(f, socket_path, logs_path, conf_path_sch, conf_path_client):
    
    f.write("#Master configure file for plms created by setup.py\n")
    f.write("socket_path:                                 %s\n"%socket_path)
    f.write("logs_path:                                   %s\n"%logs_path)
    f.write("conf_path:                                   %s\n"%conf_path_sch)
    f.write("conf_path_client:                            %s\n"%conf_path_client)
    f.write("n_proc:                                      2\n")
    #f.write("\n")
    
    
    
def main(setup_type):
    if(setup_type == "here"):
        print("Setting up Micro Python Scheduler 'here'")
        path_here = os.path.dirname(os.path.realpath(__file__)) 
        create_dir(os.path.join(path_here,".socket/"))
        create_dir(os.path.join(path_here,"logs/"))
        create_dir(os.path.join(path_here,".conf/"))
        create_dir(os.path.join(path_here,".conf/schedulers/"))
        create_dir(os.path.join(path_here,".conf/clients/"))
        create_dir(os.path.join(path_here,"bin/"))

        
        f = open((os.path.join(path_here,"plms.conf")),'w')
        create_default_conf(f, os.path.join(path_here,".socket"), os.path.join(path_here,"logs"), os.path.join(path_here,".conf/schedulers"),os.path.join(path_here,".conf/clients"))
        os.symlink(os.path.join(path_here,"client.py"), os.path.join(path_here, "bin","plms"))
        os.symlink(os.path.join(path_here,"startScheduler.py"), os.path.join(path_here, "bin","start-plms"))
        print('Finish the setup by putting this line in your bashrc: export PATH="%s:$PATH"'%os.path.join(path_here,"bin/"))
    else:
        print("No other setup supported yet")
        print("Aborting")



if(__name__ == '__main__'):
    narg = len(sys.argv)
    if(narg ==2):
        main(sys.argv[1])
    else:
        if(narg==1):
            main("here")
        else:
            print("To many arguments...")
            print("Usage: setup.py [option]")
    