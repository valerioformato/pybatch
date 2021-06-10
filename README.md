# pybatch
_pybatch_ is a tool to automate job submission for LSF and HTCondor batch systems.
It consists mainly in a bunch of
python components the main one (`launcher.py`) managing the preparation
of the jobs to be submitted and the communication with an external database
where we store information on all the jobs currently running/submitted or already
processed.

It is designed as to allow users to collaborate in one big task even among
different production sites.

`launcher.py` has many command line options, some of which are mandatory:
```
$ python launcher.py --help
usage: launcher.py [-h] [-f FILE [FILE ...]] [-k KEYTAB] [-b BATCH] [-w]
                   [--hadd] [-q QUEUE] [-p PORT] [-H DBHOST] [-U DBUSER]
                   [-P DBPASS] [--condorname CONDORNAME]
                   [--condorpool CONDORPOOL] [-bg] [-t] [-g]
```

 * `-h` displays help info
 * `-f` **MANDATORY** - Task description file(s). This file contains information on
   the task to be run, which executable, which arguments, which dataset, and so on...
   (description below)
 * `-b` **MANDATORY** - Which batch system to use (`lsf` or `condor`).
 * `-q` **MANDATORY** - The queue where the jobs will be submitted (for LSF).
 * `-U` **MANDATORY** - The database user for logging to the MongoDB instance.
 * `-P` **MANDATORY** - The user password for logging to the MongoDB instance.
 * `-H` The address of the MongoDB host (we plan to use a dedicated VM, for now it's
   `131.154.96.147`). If none the localhost will be used (not guaranteed to work...).
 * `-w` - By default a job is not submitted of the output rootfile already
   exists in the chosen directory. This flag overrides this behaviour.
 * `-bg` - Send the process in background as a daemon.
 * `-p` - HTTP port to use for steering the launcher. (default: the lowest available
   port after port 8080)
 * `-t` - Test mode, does not submit any job, instead it prints the sub command.
 * `-g` - Enable debugging output.

Some of the options are strictly related to HTCondor and still in testing phase.

#### Task file
The task is fully described by a textfile, following the [Yaml syntax](https://en.wikipedia.org/wiki/YAML).
The main field is the `task` field, inside which you can specify the following
properties:
 * `name` - The task name. This is the true identifier of the task in the DB
 * `executable` - The executable to run. Absolute path is preferrable, otherwise the relative
   path is considered from the workdir.
 * `workdir` - The working dir for the production. This is actually a legacy
   argument, but it's still mandatory. It will be removed in future versions.
 * `job` - The job template to be submitted. You can now create your own
   job template and use it for submission. It should be derived from the
   standard job.sh but then you can modify the call to your executable as you like.
 * `dataset` - The dataset identifier. Runlists are no longer on textfiles,
   but they reside in the DB instead. This removes the need for runlist sync between
   different users and different production sites.
 * `indir` - The path where the input files are located. This
   will be prepended to the single run files when passed to the executable.
 * `outdir` - The path where the output rootfiles will be copied.
 * `setenv` - The bash script that sets the environment for the job.
 * `maxjobs` - The maximum number of jobs to keep submitted to LSF (default=1000,
   this accounts for running + pending jobs). Beware that if a user submits a job
   that job is considered taken by that user, even if the job is pending, and it
   won't be submitted by another user under the same task. This means that submitting
   the same runlist entirely at once prevents collaborative processing. Please choose
   this number high enough so you are not wasting resources (enough that you always
   have some pending jobs while you maxed out your running share) but not too high
   to prevent other users from using efficiently their share.

`pybatch` is smart enough to recognize env variables in the Yaml file and expand them
at runtime ;)

#### Launcher steering
The `launcher.py` program also spawns a HTTP server listening on some port.
By default the port is 8080 but if it's in use it tries 8081, then 8082 until
the first free port avaliable. It's useful to take note of the submission host
and of the port at the moment of submission.
Using `curl` it is possible to steer the launcher with a POST request,
specifying a 'action' field
```
curl -X POST $hostname:$port/process -F"action=$ACTION" -F"additionalfield=$FIELD"
```
At the moment the following actions are available:
 * `KILL` - Forces the termination of the process.
 * `STOP` - Manually stops submission.
 * `RESUME` - Resumes default behaviour (never tested...)
 * `SWITCHQ` - Switches all pending and future jobs to the queue specified by the additional `QUEUE` field.
   This can take some time if there are a lot of jobs pending.
 * `CHANGEMAXJ` - Change the limit of jobs submitted to LSF, specified by the additional `MAXJOBS` field.
 * `CHECK` - Prints info on the number of jobs pending/running.

This is primitive and rudimentary, in an ideal world it will be replaced by
a web interface. Which means it'll never happen.

The job that will actually be submitted is the `job.sh` script. This
should act as a template, showing how a job communicates with the DB, and
how it catches the exit status of the main executable to determine if it run
smoothly or not. It will be extremely recommended to only change
the line where the executable is called, and leave everything else intact.

#### Example

That said, let's give an example.

In order to use pybatch:
 * You need the binaries for MongoDB (located at `/storage/gpfs_ams/ams/users/vformato/installed/bin`). Export your PATH accordingly.
 * Python version >2.7 is needed. It can be found on lcg `export PATH=/cvmfs/sft.cern.ch/lcg/external/Python/2.7.4/x86_64-slc6-gcc48-opt/bin/:$PATH`. (It will be
   added to the setenvs.)
 * You need the pymongo interface. It can be installed by `pip install --user pymongo`.
   In the latter case you need to export your PYTHONPATH variable to point to the local installation (usually in
   `~/.local/lib/python2.6/site-packages/:/usr/lib64/python2.6/site-packages/` on SLC6 machines, but please check it to be sure).

Let's assume that you want to run The L1 pickup efficiency, on both data and MC.

Now, if you don't have an account on the MongoDB server, [just ask!](mailto:valerio.formato@cern.ch).
An account will be created for you with a temporary randomly-generated password. You can then use the
change_pwd.py script to change it to whatever you like. Let's say you have user and password in the
`$MyMongoUser` and `$MyMongoPassword` variables.

Now let's assume that you have the repository location in the
`$REPO` variable, and you setup a simple submission script (`launchprod.sh`):
```bash
#!/bin/bash
if [ -z "$1" ]; then
  echo "Input queue as first argument"
  exit 1
fi

queue=$1

export AMSVAR=/storage/gpfs_ams/ams/users/vformato/AMS/amsvar.sh
export REPO=/storage/gpfs_ams/ams/users/vformato/Analysis/IonFluxes
export DBHOST=131.154.96.147
export DBUSER=$MyMongoUser
export DBPWD=$MyMongoPassword
export PATH=/cvmfs/sft.cern.ch/lcg/external/Python/2.7.4/x86_64-slc6-gcc48-opt/bin/:$PATH
export OUTDIR=$PWD/../histos

tasks="tasks/Data/Carbon.yaml tasks/MC/Carbon.yaml"

python $REPO/pybatch/launcher.py --hadd -f $tasks -U $DBUSER -P $DBPWD -H $DBHOST -q $queue -b lsf -w -g &> log_$queue.txt
```

Now we need to specify our task in the taskfiles `task/Data/Carbon.yaml` and `task/MC/Carbon.yaml`:
```yaml
# -- task/Data/Carbon.yaml --

#YAML template for pybatch
# V. Formato 23/10/2017
#
# You can use env variables, as $VAR
# they will be evaluated at runtime

task:
  name: L1PkEff6_VF
  executable: $REPO/DataMCComparison/L1PickUpEff/L1PickupEff_v2
  arguments: -c6
  workdir: $PWD/../
  job: job_Analysis.sh
  ntuple: ISS.B1130/pass7
  indir: /storage/gpfs_ams/ams/groups/PGIons/processed_runs_v2r5/
  outdir: $OUTDIR/Carbon
  setenv: $AMSVAR
  maxjobs: 2000
```

```yaml
# -- task/MC/Carbon.yaml --

#YAML template for pybatch
# V. Formato 23/10/2017
#
# You can use env variables, as $VAR
# they will be evaluated at runtime

task:
  name: L1PkEff6_VF
  executable: $REPO/DataMCComparison/L1PickUpEff/L1PickupEff_v2
  arguments: -c6 -M -R $REPO/EventSelection/Reweight_Fluxes/Carbon_AMS.root
  workdir: $PWD/../
  job: job_Analysis.sh
  ntuple: C.B1200
  indir: /storage/gpfs_ams/ams/groups/PGIons/processed_runs_v2r5/
  outdir: $OUTDIR/Carbon
  setenv: $AMSVAR
  maxjobs: 2000
```

This script should start the submission on the queue that you pass it, of 2000 jobs from the `ISS.B1130/pass7`
and `C.B1200` dataset, keeping at most 2000 jobs in LSF at any given time, running `$REPO/DataMCComparison/L1PickUpEff/L1PickupEff_v2` on every run
of the dataset, where the input file is located in `/storage/gpfs_ams/ams/groups/PGIons/processed_runs_v2r5/`
and the output file will be in `$OUTDIR/Carbon` directory
and all the jobs will belong to the `L1PkEff6_VF` task. And it will print additional
debug output.

To run on the `ams` queue
```
nohup ./launchprod.sh ams &
```
and if you want you can follow the submission with
```
tail -f log_ams.txt
```
