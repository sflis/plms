Python Local Micro Scheduler (plms)
===================================


plms is a lightweight scheduler for local use.


Dependencies
------------
plms has the following dependencies: `zmq <http://zeromq.org/>`_

Setup
-----
These are the steps to set up, start and use the Python Local Micro Scheduler (plms):

1. Run 'python setup.py here' which sets up the necessary folders and symlinks needed. Right
   now the only setup option is here which is in the current project directory.
2. In your .bash_profile you can add the bin folder path to your PATH. This enables you to
   run the scheduler simply by the command 'plms' and start the scheduler daemon with start-plms.
3. To start a scheduler with a default configuration one simply runs:
   'start-pmls -d -n'
   which simply means that you want the scheduler to be a daemon and you use the default configuration.
   The default configuration means that the socket files will be placed in a directory called .sockets in the plms folder, the
   scheduler name will be the same as the hostname and the maximum number of simultaneous jobs is set to 2.
   If you want to change the configuration you can edit the configuration file in the .conf/scheduler folder in the 
   project folder.
