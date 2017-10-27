# pybatch
_pybatch_ is a tool to automate job submission for LSF batch systems (with
future efforts to supporto also HTCondor). It consists mainly in a bunch of
python components the main one (```launcher.py```) managing the preparation
of the jobs to be submitted and the communication with an external database
where we store information on all the jobs currently running/submitted or already
processed.

It is designed as to allow users to collaborate in one big task even among
different production sites.

```launcher.py``` has many command line options, some of which are mandatory:
```
$ python launcher.py --help
usage: launcher.py [-h] [-T TASK] [-e EXE]
                   [-ad [EXEARGSDATA [EXEARGSDATA ...]]]
                   [-am [EXEARGSMC [EXEARGSMC ...]]] [-c CWD]
                   [-d DATASET [DATASET ...]] [-w] [-i INDIR] [-o OUTDIR]
                   [--hadd] [-q QUEUE] [-a AMSVAR] [-p PORT] [-H DBHOST]
                   [-U DBUSER] [-P DBPASS] [-m MAXJOBS] [-bg] [-t] [-g]
```

 * ```-h``` displays help info
 * ```-T``` **MANDATORY** - Sets the task name. This is important because the same
   job won't be submitted twice under the same task. This is how we ensure cooperation.
   If a user wants to run independently of any other user, he must choose a unique
   task name.
 * ```-c``` **MANDATORY** - The working directory. This is where the runlist directory should be located
   (a symbolic link is probably the smartest option here...). In here the job logs from LSF will be saved.
 * ```-e``` **MANDATORY** - The executable to be run on the working nodes.
 * ```-d``` **MANDATORY** - The dataset to be processed. This string determines which runlists
   will be used for the jobs preparation (note the plural "runlists"). All
   the runlists that contain this string will be included. For MC it should
   be possible to specify a particular production (e.g. ```C.B1113```) or even
   a specific tuning of the given production (e.g. ```C.B1113.3_00```), all the
   matching runlists will be selected. Multiple datasets can be passed together
   separated by a simple whitespace.
 * ```-i``` **MANDATORY** - The path where the input files are located. This
   will be prepended to the single run files when passed to the executable.
 * ```-o``` **MANDATORY** - The path where the output rootfiles will be copied.
 * ```-q``` **MANDATORY** - The queue where the jobs will be submitted.
 * ```-a``` **MANDATORY** - The bash script that sets the environment for the job.
 * ```-U``` **MANDATORY** - The database user for logging to the MongoDB instance.
 * ```-P``` **MANDATORY** - The user password for logging to the MongoDB instance.
 * ```-H``` The address of the MongoDB host (we plan to use a dedicated VM, for now it's
   ```amsmongodb.cern.ch```). If none the localhost will be used (not guaranteed to work...).
 * ```-b``` **MANDATORY** - The batch system to use (lsf/condor)
 * ```-m``` - The maximum number of jobs to keep submitted to LSF (default=1000, this accounts for running + pending jobs).
   Beware that if a user submits a job that job is considered *taken* by that user, even if
   the job is pending, and it won't be submitted by another user under the same task.
   This means that submitting the same runlist entirely at once prevents collaborative
   processing. Please choose this number high enough so you are not wasting resources
   (enough that you always have some pending jobs while you maxed out your running share)
   but not too high to prevent other users from using efficiently their share.
 * ```-ad``` - Additional arguments to pass to the executable to be run. They
   must be stripped of the leading '-', and values should be passed with '='.
   This flag applies to those datasets which ARE NOT montecarlo.
 * ```-am``` - Additional arguments to pass to the executable to be run. They
   must be stripped of the leading '-', and values should be passed with '='.
   This flag applies to those datasets which ARE montecarlo.
 * ```-w``` - By default a job is not submitted of the output rootfile already
   exists in the chosen directory. This flag overrides this behaviour.
 * ```-bg``` - Send the process in background as a daemon. (does this work on lxplus??)
 * ```-p``` - HTTP port to use for steering the launcher. (default: the lowest available
   port after port 8080)
 * ```-t``` - Test mode, does not submit any job, instead it prints the sub command.
 * ```-g``` - Enable debugging output.

The ```launcher.py``` program also spawns a HTTP server listening on some port.
By default the port is 8080 but if it's in use it tries 8081, then 8082 until
the first free port avaliable. It's useful to take note of the submission host
and of the port at the moment of submission.
Using ```curl``` it is possible to steer the launcher with a POST request,
specifying a 'action' field
```
curl -X POST $hostname:$port/process -F"action=$ACTION" -F"additionalfield=$FIELD"
```
At the moment the following actions are available:
 * ```KILL``` - Forces the termination of the process.
 * ```STOP``` - Manually stops submission.
 * ```RESUME``` - Resumes default behaviour (never tested...)
 * ```SWITCHQ``` - Switches all pending and future jobs to the queue specified by the additional ```QUEUE``` field.
   This can take some time if there are a lot of jobs pending.
 * ```CHANGEMAXJ``` - Change the limit of jobs submitted to LSF, specified by the additional ```MAXJOBS``` field.
 * ```CHECK``` - Prints info on the number of jobs pending/running.

This is primitive and rudimentary, in an ideal world it will be replaced by
a web interface. Which means it'll never happen.

The job that will actually be submitted is the ```job.sh``` script. This
should act as a template, showing how a job communicates with the DB, and
how it catches the exit status of the main executable to determine if it run
smoothly or not. At the moment the job template is tailored to the NtpMaker syntax
in a future release it will be possible to provide a custom written job template
to accomodate different necessities. It will be extremely reccommended to only change
the line where the executable is called, and leave everything else intact.

#### Example

That said let's give an example.

In order to use pybatch:
 * You need the binaries for MongoDB. Export your PATH accordingly.
 * Python version >2.7 is needed. It can be found on lcg ```export PATH=/cvmfs/sft.cern.ch/lcg/external/Python/2.7.4/x86_64-slc6-gcc48-opt/bin/:$PATH```.
 * You need the pymongo interface. It can be installed by ```pip install --user pymongo```.
   In the latter case you need to export your PYTHONPATH variable to point to the local installation (usually in
   ```~/.local/lib/python2.6/site-packages/``` on SLC6 machines, but please check it to be sure).

Let's assume that you want to run NtpMaker, on a test runlist with 500 runs that is available
on the common eos space (```/eos/ams/group/dbar/runlist/ISS.B950/testpass6_500.txt```).
You should have the executable in the ```bin``` directory of your CWD. Also in your
CWD you could just link the runlist directory and call it ```.filelists``` (this should be changed in the future)
```
ln -s /eos/ams/group/dbar/runlist .filelists
```
Now, if you don't have an account on the MongoDB server, [just ask!](mailto:valerio.formato@cern.ch).
An account will be created for you with a temporary randomly-generated password. You can then use the
change_pwd.py script to change it to whatever you like. Let's say you have host, user and password in the $dbhost, $dbuser and $dbpassword variables.

Now let's assume that you have the 'pybatch' repository location in the
$pybrepo variable.
Now try
```
python $pybrepo/pybatch/launcher.py -e bin/NtpMaker -c $PWD -i /eos/ams/Data/AMS02/2014/ISS.B950/pass6/ -o test_out -d ISS.B950/testpass6_500 -T test -U $dbuser -P $dbpassword -H $dbhost -a $MYAMS/amsvar.sh -q ams1nd -m 200 -g
```

This should start the submission on the ams1nd queue, of 500 jobs from the ```ISS.B950/testpass6_500```
dataset, keeping at most 200 jobs in LSF at any given time, running ```bin/NtpMaker``` on every run
of the dataset, where the input file is located in ```/eos/ams/Data/AMS02/2014/ISS.B950/pass6/```
and the output file will be in ```$PWD/test_out/ISS.B950/testpass6_500``` directory
and all the jobs will belong to the ```test``` task. And it will print additional
debug output.
