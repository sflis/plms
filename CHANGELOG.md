Release Notes - Change Log
==========================


Unreleased
--------------------------------------------------------------------------------
Changes since v0.1.7

- Added option '-n' to the queue command which sets the number of jobs to be displayed.
- The printed queue now resizes the width to the terminal window.
- Added dump option to the job command which outputs the complete job description state.
- Job command now works as intended.
- When starting the scheduler it checks if the pid in the pid file exists in the process table. If not it can safely remove the file and start a new scheduler.
- Added expression evaluation for selecting jobs. This new feature is enabled for the job, remove, resubmit commands so far.
- When the client times out no ugly python trace-backs are printed out.
- Switched to ipc sockets for communication between client and server. In the future remote might be reintroduced but for now it is not safe enough.
- Added shortcuts to the job command to quickly print out the job command or/and the run time of a job.
- Fixed inconsistencies in expression parsing.
- The rm command now returns color coded messages and states clearly if no jobs were removed.
- When server is not run as a daemon the output is directed to std-out.
- More updates to the help messages.
- If no argument is passed to the plms client it outputs the help message.

v0.1.7 Release 2015-05-19
--------------------------------------------------------------------------------
Added change log and release notes. Changes since v0.1.6

- added version numbers (not used yet)
- added a fail status for jobs which indicates whether a job had an exit code !=0.
- added warning if the user was trying to print a large file through he log command.
- removed the deprecated jdf-submit command and the non-functioning avgloadl command.
- help message fixes.
- updated the README file to .rst format.
- the job command is now able to query jobs. However only the queue fields are accessible for now.
