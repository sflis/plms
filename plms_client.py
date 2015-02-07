#!/usr/bin/env python
import os
import sys
from plms import client 
from plms.client import main
from plms.utils import parse_opt 
if(__name__ == '__main__'):
       
    #-----------------------------------------------------------------------
    # Get path to the file and initialize client.
    path_here = os.path.dirname(os.path.realpath(__file__))
    client = client.Client(path_here)

    #-----------------------------------------------------------------------
    # Get the script's input parameters from the the command line.
    if(parse_opt(sys.argv[1:2],'h')):
        print(client.print_help())
        client.save_state()
        sys.exit(0)
    command = sys.argv[1]
    if(len(sys.argv) >= 3):
        options = list(sys.argv[2:])
    else:
        options = None
        
    #path_here = os.path.dirname(os.path.realpath(__file__))
    main(command, options, client)
    client.save_state()