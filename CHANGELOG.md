Release Notes - Change Log
==========================


Unreleased
--------------------------------------------------------------------------------
Changes since v0.1.7

- added expression evaluation for selecting jobs. This new feature is enabled for the job, remove, resubmit commands so far.
- when the client times out no ugly python trace-backs are printed out.


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
