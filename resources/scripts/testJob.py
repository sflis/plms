#!/usr/bin/env python

import numpy as np
import time
import sys
import glob
import os
if(__name__ == '__main__'):
    
    scale = sys.argv[1]
    files = sys.argv[2:]
    print(files[0])
    print(glob.glob(files[0]))
    delay= float(scale)*np.random.uniform()
    print(asd)
    time.sleep(int(delay))
    print("This is a test job with",delay,"delay.")
    print(os.environ.keys())
    print(os.environ)
    