import os
import sys

def typeset_conf(conf):
    for k,v in conf.iteritems():
        if('path' in k):
            conf[k] = os.path.expanduser(v[0])
        elif('limit' in k):
            conf[k] = int(v[0])
        elif('state' in k):
            if(v[0] == 'True' or v[0] == 'true'):
                conf[k] = True
            else:
                conf[k] = False
        else:
            conf[k] = v[0]
    return conf

def read_configuration(file):
    fo= open(file,'r')
    lines =fo.readlines()
    conf = dict()
    for line in lines:
        s = line[:line.find('#')]
        if(len(s)==0):
            continue
        s = s.split(':', 1)
        conf[s[0]] = s[1].split()
    return conf

def get_configuration(file):
    return typeset_conf(read_configuration(file))


def write_configuration(file,conf):
    f = open(file,'w')
    f.write("#Micro python scheduler configuration file \n")
    f.write("#This file was automatically generated when the scheduler with this name was\n")
    f.write("#was started for the first time. This file will be read each time the     \n")
    f.write("#scheduler is started and the settings will be configured from this file. \n")
    for k, v in conf.iteritems():
        f.write("%-20s %s\n"%("%s:"%k,str(v)))
    f.close()
