#!/usr/bin/env python

import numpy as np
import time
import sys
if(__name__ == '__main__'):
    
    scale = sys.argv[1]
    files = sys.argv[2:]
    print(files)
    delay= float(scale)*np.random.uniform()
    time.sleep(int(delay))
    print("This is a test job with",delay,"delay.")