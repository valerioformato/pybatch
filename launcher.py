import pprint
import os
import sys
import signal
import string
import re
import time
import datetime
import glob
import fcntl
import subprocess
import hashlib
import argparse
import logging
import threading
import collections
import platform
import socket
import math
import pymongo

import parser

import Queue

from cStringIO import StringIO
from job.Task import Task
from job.SubTask import SubTask
from job.LSFJob import LSFJob
from job.HTCondorJob import HTCondorJob
from dbmgr import MongoDBManager
from bottle import route, run, post, request, response, default_app
from itertools import (takewhile, repeat)
# from pathos.multiprocessing import ProcessingPool as Pool
from pathos.multiprocessing import Pool
from multiprocessing import Value, Process, Manager

from ctypes import addressof

scriptdir = os.path.dirname(os.path.realpath(__file__))
UMASK = 0
WORKDIR = scriptdir

qToSubmit = collections.deque()
qRemoved = []

manager = Manager()


class States:
    SUBMIT, WAIT, FORCEWAIT, RESUME, REDUCE, FINISH = range(6)
    NAMES = ["SUBMIT", "WAIT", "FORCEWAIT", "RESUME", "REDUCE", "FINISH"]


globalParams = manager.dict()
globalParams["STATE"] = States.WAIT
globalParams["nPending"] = 0
globalParams["nRunning"] = 0
globalParams["nPendingTot"] = 0
globalParams["nRunningTot"] = 0
globalParams["limit"] = 0
globalParams["userlimit"] = 0

currx509proxy = None

BackFillResultUsed = False
subTasks = []


def daemonize():
    # Daemon-izing the process. Look here for more details: http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
    try:
        pid = os.fork()
    except OSError, e:
        raise Exception, "%s [%d]" % (e.strerror, e.errno)

    if (pid == 0):   # The first child.

        os.setsid()

        try:
            pid = os.fork()    # Fork a second child.
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if (pid == 0):    # The second child.
            # os.chdir(WORKDIR)
            os.umask(UMASK)
        else:
            os._exit(0)    # Exit parent (the first child) of the second child.
    else:
        os._exit(0)   # Exit parent of the first child.

    procParams = """
Process daemon-ized:
    process ID = %s
    parent process ID = %s
    process group ID = %s
    session ID = %s
    user ID = %s
    effective user ID = %s
    real group ID = %s
    effective group ID = %s
    """ % (os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0),
           os.getuid(), os.geteuid(), os.getgid(), os.getegid())

    print procParams


def rawincount(filename):
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.read(1024*1024) for _ in repeat(None)))
    return sum(buf.count(b'\n') for buf in bufgen)

#----------------------------------------------------------------#
# Check if a file exists via XROOTD
#----------------------------------------------------------------#


def XrdLs(file):
    match = re.search('root://(.*)/(/.*)', file)
    if not match:
        logging.error(file + " is not a valid XROOTD path")
        return False

    host = match.group(1)
    path = match.group(2)

    proc = subprocess.Popen(["xrdfs {} stat {}".format(
        host, path)], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    procOut = proc.stdout.read().strip()
    procErr = proc.stderr.read().strip()

    if procOut and (not procErr):
        return True
    elif procErr and (not procOut):
        return False
    else:
        logging.error("Something went wrong in XrdLs")
        logging.error(" - stdout: ")
        logging.error(procOut)
        logging.error(" - stderr: ")
        logging.error(procErr)

    return False


#----------------------------------------------------------------#
# Renew the last used x509 proxy
#----------------------------------------------------------------#
def RenewX509Proxy():
    global virtOrg
    global vomsDir
    global currx509proxy
    # global STATE

    while not currx509proxy:
        time.sleep(1)

    while True:
        if globalParams["STATE"] == States.FINISH:
            return
        cmd = "voms-proxy-init --voms {} --valid 24:0 --out {}".format(
            virtOrg, currx509proxy)
        if vomsDir:
            cmd += " --vomsdir {}".format(vomsDir)

        logging.info("Renewing x509 proxy")

        proc = subprocess.Popen(
            [cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        procOut = proc.stdout.read().strip()
        procErr = proc.stderr.read().strip()

        logging.info(procOut)
        logging.info(procErr)

        time.sleep(60 * 60 * 6 - 1)


#----------------------------------------------------------------#
# Code to find filelists for datasets
#----------------------------------------------------------------#
def GetFilelists():

    # global isMC, inlist, runlist
    global tasks
    print tasks
    for task in tasks:

        if (not task.workdir.endswith('/') and len(task.workdir) > 0):
            task.workdir += '/'
            pass

        if task.isNtuples:
            collection = db.ntuples
        else:
            collection = db.datasets

        for subtask in task.subtasks:
            dataset = subtask.name
            if "pass" in dataset:
                subtask.isMC = False
                print "Getting filelists for", dataset, "querying DB..."
                query = collection.find({u'dataset': dataset}, {u'subset': 1})
                subtask.subsets = [dset['subset'] for dset in query]
                pass
            else:
                subtask.isMC = True
                mcbuildlist = dataset.split('/')
                subtask.name = mcbuildlist[0]
                if len(mcbuildlist) == 1:
                    print "Getting runlists for MC production", mcbuildlist[0]
                    query = collection.find(
                        {u'dataset': mcbuildlist[0]}, {u'subset': 1})
                    subtask.subsets = [dset['subset'] for dset in query]
                    pass
                else:
                    print "Getting runlists for MC production", mcbuildlist[0], "subset", mcbuildlist[1]
                    query = collection.find({
                        u'dataset': mcbuildlist[0],
                        u'subset': {u'$regex': ".*{}".format(mcbuildlist[1])}
                    }, {
                        u'subset': 1
                    })
                    subtask.subsets = [dset['subset'] for dset in query]
                    pass
            print ' - %s \n' % '\n - '.join(map(str, subtask.subsets))

            for subset in subtask.subsets:
                subtask.indir.append(task.indir + subset + "/")

#----------------------------------------------------------------#
# Code to get outdir names
#----------------------------------------------------------------#


def GetOutDir():

    logging.info("Setting output directories")
    for task in tasks:
        # dataset = task.name
        for subtask in task.subtasks:
            _outdir = task.outdir
            # print iset, _outdir, dataset
            if (not _outdir.startswith("/")) and (not _outdir.startswith("root://")):
                _outdir = os.getcwd() + '/' + _outdir
                pass
            for subset in subtask.subsets:
                subtask.outdir.append(str(_outdir+'/'+subset))
                print " - " + str(_outdir+'/'+subset)
                pass
            pass

    print "\n"

#----------------------------------------------------------------#
# Code to setup global variables
#----------------------------------------------------------------#


def SetupInputPar():
    global hostname
    hostname = str(subprocess.Popen(
        ['hostname', ''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip())

    global useCWD
    useCWD = True

    global logLevel
    logLevel = logging.INFO
    if args.debug:
        logLevel = logging.DEBUG
    logging.basicConfig(level=logLevel,
                        format='[%(levelname)s] (%(threadName)s) %(message)s',
                        )

    #----------------------------------------------------------------#
    # Get YAML file
    #----------------------------------------------------------------#
    if not args.file:
        logging.error(
            "You need to provide at least a YAML file for the task definition (-f option)")
        sys.exit(1)
        pass

    global taskfiles
    taskfiles = args.file

    #----------------------------------------------------------------#
    # Get keytab
    #----------------------------------------------------------------#
    global keytab
    if args.keytab:
        keytab = args.keytab[0]
    else:
        keytab = None

    #----------------------------------------------------------------#
    # Get MongoDB host, user and pass
    #----------------------------------------------------------------#
    if not args.dbuser:
        logging.error(
            "You need to provide a user to log on the DB (-U option)")
        sys.exit(1)
        pass

    if not args.dbpass:
        logging.error(
            "You need to provide a password to log on the DB (-P option)")
        sys.exit(1)
        pass

    global dbuser
    dbuser = str(args.dbuser[0])

    global dbpass
    dbpass = str(args.dbpass[0])

    global dbhost
    if args.dbhost:
        dbhost = str(args.dbhost[0])
    else:
        dbhost = hostname

    logging.debug("dbhost is {}".format(dbhost))

    #----------------------------------------------------------------#
    # Get accounting user
    #----------------------------------------------------------------#
    global user
    if args.user:
        user = str(args.user[0])
        logging.debug("user is {}".format(user))
    else:
        user = None

    #----------------------------------------------------------------#
    # Get batch system
    #----------------------------------------------------------------#
    if not args.batch:
        logging.error("no batch specified")
        sys.exit(1)
        pass
    global batchSystem
    if args.batch[0] == "lsf":
        batchSystem = "LSF"
    elif args.batch[0] == "condor":
        batchSystem = "HTCondor"
    else:
        logging.error("Batch system not specified or not supported")
        sys.exit(1)

    logging.info("Using {}".format(batchSystem))

    #----------------------------------------------------------------#
    # Check for additional batch parameters
    #----------------------------------------------------------------#
    if batchSystem == "HTCondor":
        global condorPool
        condorPool = None
        if args.condorpool:
            condorPool = args.condorpool[0]

        global condorName
        condorName = None
        if args.condorname:
            condorName = args.condorname[0]

        if (condorPool and not condorName):
            logging.error("You need to specify schedd name with pool...")
            sys.exit(1)

    #----------------------------------------------------------------#
    # Get queue
    #----------------------------------------------------------------#
    if not args.queue:
        logging.error("No queue specified")
        sys.exit(1)
        pass
    global queue
    queue = args.queue[0]
    logging.info("Submitting on queue {}".format(queue))

    #----------------------------------------------------------------#
    # Check reduce
    #----------------------------------------------------------------#
    if args.hadd:
        logging.info("With hadd step")
        pass

    #----------------------------------------------------------------#
    # Check if cleaning tasks
    #----------------------------------------------------------------#
    global cleanTasks
    if args.clean_tasks:
        logging.info("Cleaning existing tasks")
        cleanTasks = True
        pass
    else:
        cleanTasks = False
    #----------------------------------------------------------------#
    # Get HTTP port
    #----------------------------------------------------------------#
    global hostport
    if not args.port:
        hostport = 8080
        pass
    else:
        hostport = int(args.port[0])

    #----------------------------------------------------------------#
    # Get VO and vomsdir
    #----------------------------------------------------------------#
    global virtOrg
    if args.vo:
        virtOrg = str(args.vo[0])
    else:
        virtOrg = None

    global vomsDir
    if virtOrg:
        if args.vomsdir:
            vomsDir = str(args.vomsdir[0])
        else:
            vomsDir = None


def GetYamlParams(filename):
    yparser = parser.YamlParser()
    yparser.ImportFile(filename)

    #----------------------------------------------------------------#
    # SETUP task name, executable, working dir and job template
    #----------------------------------------------------------------#
    if not yparser.Get('name'):
        logging.error("You need to provide name for the task")
        sys.exit(1)
        pass

    if not yparser.Get('executable'):
        logging.error("You need to provide an executable")
        sys.exit(1)
        pass

    if not yparser.Get('workdir'):
        logging.error("You need to specify working directory")
        sys.exit(1)
        pass

    if not yparser.Get('job'):
        logging.error("You need to provide a job template")
        sys.exit(1)
        pass

    task = Task(yparser.Get('name'))
    task.exe = yparser.Get('executable')
    task.workdir = yparser.Get('workdir')
    task.jobscript = yparser.Get('job')

    if not yparser.Get('reducescript'):
        task.reducescript = "job_hadd.sh"
    else:
        task.reducescript = yparser.Get('reducescript')

    task.workdir = os.path.abspath(str(task.workdir))
    if not task.workdir.endswith('/'):
        task.workdir += "/"
        pass

    if not os.path.isabs(task.exe):
        task.exe = os.path.abspath(task.exe)

    if not os.path.isabs(task.jobscript):
        task.jobscript = os.path.abspath(task.jobscript)

    if task.reducescript and not os.path.isabs(task.reducescript):
        task.reducescript = os.path.abspath(task.reducescript)

    logging.debug("taskname:        {}".format(task.name))
    logging.debug("executable:      {}".format(task.exe))
    logging.debug("workdir:         {}".format(task.workdir))
    logging.debug("job template:    {}".format(task.jobscript))
    if task.reducescript:
        logging.debug("reduce template: {}".format(task.reducescript))

    #----------------------------------------------------------------#
    # Get datasets
    #----------------------------------------------------------------#
    _datasets = yparser.Get('dataset')
    _ntuples = yparser.Get('ntuple')

    if not _datasets and not _ntuples:
        logging.error("No dataset nor ntuple specified")
        sys.exit(1)
        pass

    if _datasets and _ntuples:
        logging.error(
            "You specified both dataset and ntuple. Sure about what you're doing?")
        sys.exit(1)
        pass

    task.isNtuples = False
    if not _datasets and _ntuples:
        task.isNtuples = True
        _list = _ntuples.split(' ')
        pass
    else:
        task.isNtuples = False
        _list = _datasets.split(' ')
        pass

    # datasets = datasets.split(' ')
    for dataset in _list:
        task.subtasks.append(SubTask(dataset, task))

    #----------------------------------------------------------------#
    # Check if executable exists
    #----------------------------------------------------------------#
    if not os.path.isfile(task.exe):
        logging.error("Executable {} not found".format(task.exe))
        # sys.exit(1)
    logging.info("Executable to run: {}".format(task.exe))

    #----------------------------------------------------------------#
    # Get cl arguments for the executable
    #----------------------------------------------------------------#
    argstring = yparser.Get('arguments')
    if not argstring:
        argstring = ""
        pass

    # if args.overwrite:
    #     argstring   += " -w"
    #     pass

    task.exeargs = argstring

    #----------------------------------------------------------------#
    # Get amsvar
    #----------------------------------------------------------------#
    amsvar = yparser.Get('setenv')
    if not amsvar:
        logging.error("Please provide amsvar script")
        sys.exit(1)
        pass

    amsvar = os.path.abspath(str(amsvar))
    if not os.path.isfile(amsvar):
        logging.error("amsvar script not found")
        sys.exit(1)
        pass

    task.amsvar = amsvar
    logging.info("Using amsvar script: {}".format(amsvar))

    #----------------------------------------------------------------#
    # Get x509userproxy
    #----------------------------------------------------------------#
    x509userproxy = yparser.Get('x509userproxy')
    if not x509userproxy:
        if virtOrg:
            task.x509userproxy = "$PWD/pbx509proxy"
        else:
            task.x509userproxy = ""
        pass
    else:
        x509userproxy = os.path.abspath(str(x509userproxy))
        if not os.path.isfile(x509userproxy):
            logging.error("x509userproxy file not found")
            sys.exit(1)
            pass

        task.x509userproxy = x509userproxy
        logging.info("Using x509 user proxy: {}".format(x509userproxy))

    #----------------------------------------------------------------#
    # Get indir and outdir
    #----------------------------------------------------------------#
    _indir = yparser.Get('indir')
    _outdir = yparser.Get('outdir')

    if not _indir:
        logging.error("No input directory specified")
        sys.exit(1)
        pass

    if not _outdir:
        logging.error("No output directory specified")
        sys.exit(1)
        pass

    if not _indir.endswith('/'):
        _indir += '/'

    task.indir = _indir
    task.outdir = _outdir

    #----------------------------------------------------------------#
    # Get maxjobs
    #----------------------------------------------------------------#
    _limit = yparser.Get('maxjobs')
    if not _limit:
        _limit = 1000
        pass
    else:
        _limit = int(_limit)

    task.maxjobs = _limit

    globalParams["limit"] = _limit

    return task


def SigHandler(signum, frame):
    print 'Signal handler called with signal', signum
    if signum in [signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGKILL]:
        sys.exit(1)
        pass


def QueueJobs():
    logging.info("Preparing jobs for submission...")

    global queue
    global keytab
    global condorName
    global condorPool
    global cleanTasks
    # global portLock
    # logging.debug("Acquiring portLock")
    # portLock.acquire()
    # logging.debug("...acquired")
    for task in tasks:

        if cleanTasks:
            dbMgr.CleanTaskFromDB(task.name)

        for iodir in ["pin", "logs"]:
            if (not args.test) and (not os.path.exists(task.workdir+iodir)):
                os.makedirs(task.workdir+iodir)
                try:
                    os.chmod(task.workdir+iodir, 0775)
                except OSError:
                    pass
                pass
            pass

        # Check jobscript
        if not os.path.isfile(task.jobscript):
            if os.path.isfile(scriptdir + '/' + os.path.basename(task.jobscript)):
                logging.warning("Warning: file {} not found. Reverting to the one in {}".format(
                    task.jobscript, scriptdir))
                task.jobscript = scriptdir + '/' + \
                    os.path.basename(task.jobscript)
            else:
                logging.warning(
                    "Warning: file {} not found. Reverting to default one.".format(task.jobscript))
                task.jobscript = scriptdir+'/'+"job.sh"
                # sys.exit(2)
            pass

        # Check reducescript
        if args.hadd and task.reducescript and not os.path.isfile(task.reducescript):
            if os.path.isfile(scriptdir + '/' + os.path.basename(task.reducescript)):
                logging.warning("Warning: file {} not found. Reverting to the one in {}".format(
                    task.reducescript, scriptdir))
                task.reducescript = scriptdir + '/' + \
                    os.path.basename(task.reducescript)
            else:
                logging.warning(
                    "Warning: file {} not found. Reverting to default one.".format(task.reducescript))
                task.reducescript = scriptdir+'/'+"job_hadd.sh"
                # sys.exit(2)
            pass

        if task.isNtuples:
            collection = db.ntuples
        else:
            collection = db.datasets

        for subtask in task.subtasks:
            icount = 0
            totfiles = 0
            filelist = []
            subsetlist = []

            for subset in subtask.subsets:
                query = collection.find({
                    u'dataset': subtask.name,
                    u'subset': subset
                })

                subtask.runlist.append(query[0]['runlist'])
                subsetlist.append(subset)

            for ilist in range(0, len(subtask.runlist)):
                totfiles += len(subtask.runlist[ilist])
            subtask.totJobs = totfiles

            for ilist in range(0, len(subtask.subsets)):

                print "\nSubmitting jobs for subset", subtask.subsets[
                    ilist], "jobs on queue", queue, "\n"

                odir = subtask.outdir[ilist]
                if (not args.test) and (not os.path.exists(odir) and (not odir.startswith("root://"))):
                    print "Creating output dir", odir
                    try:
                        os.makedirs(odir)
                        os.makedirs(odir+'/logs')
                        os.chmod(odir+"/..", 0775)
                        os.chmod(odir, 0775)
                        os.chmod(odir+"/logs", 0775)
                    except OSError:
                        pass
                    pass
                pass

                old_perc_progress = 0

                for ifile in subtask.runlist[ilist]:
                    sys.stdout.flush()
                    while len(qToSubmit) > 2000:
                        time.sleep(1)
                        pass

                    icount += 1

                    perc_progress = math.floor(100*icount/totfiles)
                    if perc_progress != old_perc_progress:
                        sys.stdout.write(
                            "Loading runlist... %d%%   \r" % (perc_progress))
                        sys.stdout.flush()
                        old_perc_progress = perc_progress

                    #----------------------------------------------------------------#
                    # Get outfile
                    #----------------------------------------------------------------#
                    ofile = ifile['run'] + ".root"

                    if batchSystem == "LSF":
                        job = LSFJob(icount, subtask)
                    elif batchSystem == "HTCondor":
                        job = HTCondorJob(icount, subtask)
                        if condorName:
                            job.SetSchedd(condorName, condorPool)

                    if args.test:
                        job.test = True

                    if keytab:
                        job.keytab = keytab

                    job.useCWD = useCWD

                    job.amsvar = task.amsvar
                    job.exe = task.exe
                    job.workdir = task.workdir
                    job.arguments = task.exeargs
                    job.indir = subtask.indir[ilist]
                    job.outdir = subtask.outdir[ilist]
                    job.infile = " ".join(ifile['files'])
                    job.outfile = ofile
                    job.queue = queue
                    job.script = task.jobscript
                    job.user = user
                    job.site = hostname

                    job.FillVars()
                    job.SetName()
                    job.GetCMD()
                    # job.Dump()

                    subtask.AddJob(job)

                    #----------------------------------------------------------------#
                    # If we don't overwrite and the output file is already present
                    # we insert the job in the DB as Done
                    #----------------------------------------------------------------#
                    # If the outfile is missing but the job is marked as done
                    # we remove it, forcing resubmission
                    #----------------------------------------------------------------#
                    if not args.overwrite:
                        query = db.jobs.find_one(
                            {u'hash': job.hash, u'task': job.task.name}, {u'_id': 0, u'status': 1})
                        checkfun = XrdLs if odir.startswith(
                            "root://") else os.path.isfile
                        if checkfun(odir + '/' + ofile):
                            if query:
                                if query['status'] == 'Done':
                                    logging.info(
                                        "Job " + str(icount) + "/" + str(totfiles) + " already done. Skipping")
                                    continue
                                else:
                                    logging.error("Output file {} present but job not in Done state. Please check!!".format(
                                        odir + '/' + ofile))
                                    subtask.AddJob(job)
                                    continue
                                pass
                            else:
                                logging.info(
                                    "Job " + str(icount) + "/" + str(totfiles) + " already done. Skipping and inserting.")
                                dbMgr.InsertJob(job, u'Done', True)
                                continue
                            pass
                        else:
                            if query:
                                if query['status'] == 'Done':
                                    logging.error("Output file {} not present but job in Done state. Resubmitting!!".format(
                                        odir + '/' + ofile))
                                    db.jobs.delete_one({'hash': job.hash})
                                    pass
                                pass
                            pass
                        pass

                    #----------------------------------------------------------------#
                    # Check if job is already in DB
                    #----------------------------------------------------------------#
                    query = db.jobs.find_one(
                        {u'hash': job.hash, u'task': job.task.name})
                    if query:
                        logging.info("Job " + str(icount) + "/" +
                                     str(totfiles) + " already submitted. Skipping")
                        continue

                    subtask.AddJob(job)

                    job.queued = True
                    qToSubmit.append(job)

            # prep command for reduce step
            if args.hadd:
                if batchSystem == "HTCondor" and condorName:
                    subtask.PrepareReduceJobs(
                        batchSystem, queue, user, hostname, condorName, condorPool)
                else:
                    subtask.PrepareReduceJobs(
                        batchSystem, queue, user, hostname)

            # To be removed:
            # for _dir in subtask.outdir:
            #     reduceCmd = "bsub -q " + queue
            #     reduceCmd += " -oo " + task.workdir + "/pout/hadd_" + \
            #         string.replace(subtask.name, '/', '_') + \
            #         "_" + os.path.basename(odir+".txt") + " "
            #     reduceCmd += "\"source "+task.amsvar+"; hadd -f " + \
            #         _dir+".root" + " " + _dir + "/*.root\""
            #     subtask.postCmd.append(reduceCmd)
            # logging.debug("Reduce step prepared")
            # logging.debug(subtask.postCmd)

    global queueIsDone
    queueIsDone = True
    # portLock.release()


def PrintSummary():
    logging.info(str(len(qToSubmit)) + " jobs to be submitted")
    logging.info(str(globalParams["nPending"]) + " jobs pending")
    logging.info(str(globalParams["nRunning"]) + " jobs running")
    sys.stderr.flush()


def FindJobFromHash(qjob):
    # logging.debug("Looking for job {}".format(qjob[u'hash']))
    # task = [t for t in tasks if t.name == qjob[u'task']][0]
    for task in tasks:
        subtask_sel = [s for s in task.subtasks if s.name == qjob[u'subtask']]
        if len(subtask_sel) > 0:
            subtask = subtask_sel[0]
            jobSearch = subtask.FindJob(qjob[u'hash'])
            return jobSearch
    return None


def RemoveFinishedJob(qjob):
    task = [t for t in tasks if t.name == qjob[u'task']][0]
    subtask_sel = [s for s in task.subtasks if s.name == qjob[u'subtask']]
    if len(subtask_sel) > 0:
        subtask = subtask_sel[0]
        subtask.RemoveJob(qjob[u'hash'])
        # logging.debug("Job " + qjob[u'hash'] + " removed")
    # else:
        # logging.debug("Job " + qjob[u'hash'] + " not found")

    return

#----------------------------------------------------------------#
# Atomic action to submit one reduce action
#----------------------------------------------------------------#


def SubmitReduce(cmd):
    logging.info("Submitting reduce job")
    sys.stderr.flush()
    if (not args.test):
        # logging.debug(cmd)
        try:
            dummy = subprocess.Popen(
                [cmd, ''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip()
        except Exception, e:
            logging.error("Unexpected error: " + str(e))
        # print dummy
        logging.info("Reduce job submitted")
        sys.stderr.flush()
        pass
    else:
        print cmd


#----------------------------------------------------------------#
# Atomic action to submit one LSFJob
#----------------------------------------------------------------#
def SubmitJob(job):

    global globalParams

    global currx509proxy
    currx509proxy = job.task.x509userproxy

    jobIsRetrying = False

    #----------------------------------------------------------------#
    # Check if job is already in DB
    #----------------------------------------------------------------#
    query = db.jobs.find_one({u'hash': job.hash, u'task': job.task.name}, {
                             u'_id': 0, u'status': 1, u'user': 1, u'from': 1, u'retries': 1})
    if query:
        if query['status'] in ['Pending', 'Running']:
            print "Job", str(job.i) + "/" + str(job.totfiles), "already submitted (" + str(
                query['user']) + " on host " + str(query['from']) + "). Skipping"
            return False
        elif query['status'] == 'Done':
            if not args.overwrite:
                print "Job", str(job.i) + "/" + str(job.totfiles), "already done (" + str(
                    query['user']) + " on host " + str(query['from']) + "). Skipping"
                return False
        elif query['status'] == 'Error':
            job.retries = query['retries'] + 1
            jobIsRetrying = True

    if (not args.test):
        #----------------------------------------------------------------#
        # Submit the job
        #----------------------------------------------------------------#
        insertStatus = dbMgr.InsertJob(
            job, u'Pending', True if jobIsRetrying else False)
        if insertStatus == False:
            logging.debug("Failed DB insertion, skipping")
            return False
        print "Submitting job", str(job.i) + "/" + str(job.totfiles)
        sys.stdout.flush()
        subStatus = job.Submit()
        if not subStatus:
            return False
        insertStatus = dbMgr.InsertJob(
            job, u'Pending', True)
        if insertStatus == False:
            return False
        logging.debug("Submit status: {}, ID: {}".format(str(subStatus), job.condorID if batchSystem == "HTCondor" else job.lsfID))
        # if not dbMgr.UpdateJobWithID(job, subStatus):
        #     return False

        print "Job", str(job.i) + "/" + str(job.totfiles), "submitted"
        sys.stdout.flush()
        pass
    else:
        print "submitting one job", job.hash
        job.Submit()
        dbMgr.InsertJob(job, u'Pending', True if jobIsRetrying else False)
        pass

    return True


class SubmissionManager(object):
    def __init__(self, processes, queue_size, threads):
        # self.threads = []
        self.queue = Queue.Queue(queue_size)

        for iThr in range(0, processes):
            threads.append(threading.Thread(
                target=self.Task, name="SubmissionThread-{}".format(iThr)))
            threads[-1].start()

    def new_task(self, job):
        # logging.debug("Putting a new job")
        self.queue.put(job)

    def Task(self):
        while True:
            # global STATE
            if globalParams["STATE"] == States.FINISH:
                logging.debug("Exiting...")
                return

            job = self.queue.get()

            result = SubmitJob(job)
            if result == False:
                global target
                target += 1
            self.queue.task_done()


#----------------------------------------------------------------#
# Manual check of running jobs still being there (LSF version)
#----------------------------------------------------------------#
def CheckLSFStatus():

    time.sleep(600)

    while True:
        if globalParams["STATE"] == States.FINISH:
            logging.debug("Exiting...")
            return

        if globalParams["nRunning"] < 1 and globalParams["nPending"] < 1:
            time.sleep(0.1)
            continue

        time.sleep(10)    # wait a few of seconds, give time to LSF to
        # update the status of the jobs...
        logging.info("Checking LSF status...")
        bjcmd = subprocess.Popen(
            ["bjobs", ''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        bjout = bjcmd.stdout.read().strip()
        bjerr = bjcmd.stderr.read().strip()
        # remove first line (LSF header)
        bjout = "\n".join(bjout.splitlines()[1:])
        logging.debug("...done")
        logging.debug("out {} lines, err {} lines".format(
            len(bjout.splitlines()), len(bjerr.splitlines())))

        query = list(db.jobs.find({
            u'task': {u'$in': [x.name for x in tasks]},
            u'type': u'LSFJob',
            u'status': {u'$ne': u'Done'}
        }, {u'hash': 1, u'lsfID': 1, u'subtask': 1, u'user': 1, u'from': 1, u'status': 1, u'_id': 0}))

        templistP = [x for x in query
                     if (
                         x[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                         x[u'user'] == user and
                         x[u'from'] == hostname and
                         x[u'status'] == u'Pending' and
                         True)
                     ]

        templistR = [x for x in query
                     if (
                         x[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                         x[u'user'] == user and
                         x[u'from'] == hostname and
                         x[u'status'] == u'Running' and
                         True)
                     ]

        # parse running jobs
        logging.info("Parsing running jobs... ({})".format(len(templistR)))
        # print len(templistR), "running jobs in cache"
        for entry in templistR:
            temp = [x.split() for x in bjout.splitlines() if int(
                x.split(' ')[0]) == int(entry['lsfID'])]
            # print "Searching", entry['lsfID'], "({})".format(type(entry['lsfID'])), "=", temp
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                job = FindJobFromHash(entry)
                logging.debug(
                    "job " + str(entry['lsfID']) + " not in running state but it should")
                db.jobs.delete_one({'hash': entry['hash']})

                if job:
                    queueLockT.acquire()
                    qToSubmit.append(job)
                    queueLockT.release()
                pass
            elif line[2] == "RUN":
                logging.debug(str(entry['lsfID']) + " is in RUN state")
                continue
            elif line[2] == "PEND":
                logging.debug(str(entry['lsfID']) +
                              " was in RUN state, now in PEND")
                job = FindJobFromHash(entry)
                if job:
                    dbMgr.UpdateJobStatus(job, "Pending")
                continue

        del templistR

        # parse pending jobs
        logging.info("Parsing pending jobs...({})".format(len(templistP)))
        # print len(templistR), "running jobs in cache"
        for entry in templistP:
            temp = [x.split() for x in bjout.splitlines() if int(
                x.split(' ')[0]) == int(entry['lsfID'])]
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                job = FindJobFromHash(entry)

                if job:
                    logging.debug("job {} not in pending state but it should ({})".format(
                        str(entry['lsfID']), job.notFoundPending))

                if job and job.notFoundPending:
                    db.jobs.delete_one({'hash': entry['hash']})
                    queueLockT.acquire()
                    qToSubmit.append(job)
                    queueLockT.release()
                elif job:  # for this time just mark it, eventually we will act in a couple of minutes
                    job.notFoundPending = True
                    pass
            elif line[2] == "PEND":
                logging.debug(str(entry['lsfID']) + " is in PEND state")
                continue
            elif line[2] == "RUN":
                logging.debug(
                    "job " + str(entry['lsfID']) + " in running state but it should be pending")

        del templistP

        logging.info("...going to sleep")
        time.sleep(600)  # wait 10 minutes


#----------------------------------------------------------------#
# Manual check of running jobs still being there (HTCondor version)
#----------------------------------------------------------------#
def CheckHTCondorStatus():

    global condorName
    condor_q_cmd = "condor_q -nobatch"
    if condorName and len(condorName) > 0:
        condor_q_cmd += " -name {}".format(condorName)
    if condorPool and len(condorPool) > 0:
        condor_q_cmd += " -pool {}".format(condorPool)

    # postpone test for later...
    time.sleep(600)
    while True:
        if globalParams["STATE"] == States.FINISH:
            logging.debug("Exiting...")
            return

        if globalParams["nRunning"] < 1 and globalParams["nPending"] < 1:
            time.sleep(0.1)
            continue

        time.sleep(10)    # wait a few of seconds, give time to HTCondor to
        # update the status of the jobs...
        logging.info("Checking HTCondor status...")
        statusCmd = subprocess.Popen(
            [condor_q_cmd, ''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        bjout = statusCmd.stdout.read().strip()
        bjerr = statusCmd.stderr.read().strip()
        streamdata = statusCmd.communicate()[0]
        rc = statusCmd.returncode
        if (rc > 0) or (bjerr != ""):
            logging.error("condor_q command failed")
            time.sleep(600)
            continue

        templist_s = []
        for line in bjout.splitlines():
            if re.match('^\d.*', line):
                templist_s.append(line)
        bjout = "\n".join(templist_s)
        logging.debug("...done ({})".format(len(bjout.splitlines())))

        query = list(db.jobs.find({
            u'task': {u'$in': [x.name for x in tasks]},
            u'type': u'HTCondorJob',
            u'status': {u'$ne': u'Done'}
        }, {u'hash': 1, u'condorID': 1, u'subtask': 1, u'user': 1, u'from': 1, u'status': 1, u'_id': 0}))

        templistP = [x for x in query
                     if (
                         x[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                         x[u'user'] == user and
                         x[u'from'] == hostname and
                         x[u'status'] == u'Pending' and
                         True)
                     ]

        templistR = [x for x in query
                     if (
                         x[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                         x[u'user'] == user and
                         x[u'from'] == hostname and
                         x[u'status'] == u'Running' and
                         True)
                     ]

        # parse running jobs
        logging.info("Parsing running jobs... ({})".format(len(templistR)))
        for entry in templistR:
            temp = [x.split() for x in bjout.splitlines() if int(
                x.split(' ')[0].split('.')[0]) == int(entry['condorID'])]

            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1 or line[5] == "C":
                logging.debug(
                    "job " + str(entry['condorID']) + " not in running state but it should")
                job = FindJobFromHash(entry)
                db.jobs.delete_one({'hash': entry['hash']})

                if job:
                    queueLockT.acquire()
                    qToSubmit.append(job)
                    queueLockT.release()
                pass
            elif line[5] == "R":
                logging.debug(str(entry['condorID']) + " is in RUN state")
                continue
            elif line[5] == "I":
                logging.debug(str(entry['condorID']) +
                              " has been put to PEND state")
                job = FindJobFromHash(entry)
                if job:
                    dbMgr.UpdateJobStatus(job, "Pending")
                continue

        del templistR

        # parse pending jobs
        logging.info("Parsing pending jobs... ({})".format(len(templistP)))
        for entry in templistP:
            temp = [x.split() for x in bjout.splitlines() if int(
                x.split(' ')[0].split('.')[0]) == int(entry['condorID'])]

            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1 or line[5] == "C":
                job = FindJobFromHash(entry)

                logging.debug("job {} not in pending state but it should".format(str(entry['condorID'])))

                if job and job.notFoundPending:
                    db.jobs.delete_one({'hash': entry['hash']})
                    queueLockT.acquire()
                    qToSubmit.append(job)
                    queueLockT.release()
                elif job:  # for this time just mark it, eventually we will act in a couple of minutes
                    job.notFoundPending = True
                    pass
            elif line[5] == "I":
                logging.debug(str(entry['condorID']) + " is in PEND state")
                continue
            elif line[5] == "R":
                logging.debug(
                    "job " + str(entry['condorID']) + " in running state but it should be pending")

        del templistP

        logging.info("...going to sleep")
        time.sleep(600)  # wait 10 minutes

#----------------------------------------------------------------#
# Finite state machine
# Possible states:
#  - WAIT        (waits for 2 minutes)
#  - SUBMIT      (submits first job in queue)
#  - FORCEWAIT   (waits until a resume request)
#  - RESUME      (does nothing, just to switch from FORCEWAIT)
#  - REDUCE      (to perform hadd after all jobs done)
#  - FINISH      (gtfo)
#----------------------------------------------------------------#


def FiniteStateMachine(subMgr):
    logging.debug('start')

    global qToSubmit
    global globalParams
    global isBackFillReady
    global BackFillResultUsed
    global queueIsDone

    # global STATE
    globalParams["STATE"] = States.WAIT
    prevSTATE = globalParams["STATE"]

    time.sleep(10)  # give time to BackFill to run at least once
    while not isBackFillReady:
        time.sleep(1)

    global target
    target = 0

    # global subMgr

    while True:

        #----------------------------------------------------------------#
        # Check if we have a closed subtask
        #----------------------------------------------------------------#
        isSubTaskDone = True
        readyReduce = None
        for subtask in [y for x in tasks for y in x.subtasks if not y.isReduced]:
            isSubTaskDone = isSubTaskDone and subtask.IsDone()
            if subtask.IsDone() == True:
                readyReduce = subtask
                break
            pass

        jobsSubmitted = globalParams["nPending"] + globalParams["nRunning"]
        if BackFillResultUsed == False:
            target = (globalParams["userlimit"] if globalParams["userlimit"]
                      > 0 else globalParams["limit"]) - jobsSubmitted
            BackFillResultUsed = True

        #----------------------------------------------------------------#
        # State switch rules
        #----------------------------------------------------------------#
        if globalParams["STATE"] == States.FORCEWAIT:
            time.sleep(0.1)
            continue

        if (readyReduce != None) and args.hadd:
            logging.debug("Reduce step to be performed, set: " +
                          str(readyReduce.name))
            globalParams["STATE"] = States.REDUCE
        elif globalParams["STATE"] == States.FINISH:
            logging.info('Finished')
            return
        elif (len(qToSubmit) + globalParams["nRunningTot"] + globalParams["nPendingTot"] == 0) and (queueIsDone):
            globalParams["STATE"] = States.FINISH
        elif target <= 0:
            # Print summary when stopping submit
            if(globalParams["STATE"] == States.SUBMIT):
                PrintSummary()
                logging.debug(
                    'limit at ' + str(globalParams["userlimit"] if globalParams["userlimit"] > 0 else globalParams["limit"]))
            globalParams["STATE"] = States.WAIT
        elif len(qToSubmit) > 0:
            globalParams["STATE"] = States.SUBMIT
        else:
            if prevSTATE == States.SUBMIT:
                PrintSummary()
            globalParams["STATE"] = States.WAIT
        newSTATE = globalParams["STATE"]

        if args.debug:
            if newSTATE != prevSTATE:
                logging.debug(
                    "Going from " + States.NAMES[prevSTATE] + " to " + States.NAMES[newSTATE])

        #----------------------------------------------------------------#
        # Map state to action
        #----------------------------------------------------------------#
        if globalParams["STATE"] == States.WAIT:
            time.sleep(1)
        elif globalParams["STATE"] == States.SUBMIT:
            job = qToSubmit.popleft()
            logging.debug(
                "Target = {}; job {}/{}".format(target, job.i, job.totfiles))
            globalParams["limit"] = next(
                (x.maxjobs for x in tasks if x.name == job.task.name), 1000)
            job.queued = False

            subMgr.new_task(job)
            target -= 1
        elif globalParams["STATE"] == States.RESUME:
            pass                         # Just go to the next tick
        elif globalParams["STATE"] == States.REDUCE:
            logging.debug("We are in REDUCE state, set " +
                          str(readyReduce.name))
            for reduceJob in readyReduce.reduceJobs:
                subMgr.new_task(reduceJob)
            readyReduce.isReduced = True

        prevSTATE = globalParams["STATE"]
        pass

        time.sleep(0.01)

#----------------------------------------------------------------#
# HTTP listener daemon
#----------------------------------------------------------------#


def Listener(globalParams):
    logging.debug('start')
    global hostport

    @post('/process')
    def handle_request():
        # global STATE
        action = request.forms.get('action')
        status = request.forms.get('status')

        returnString = ""

        #----------------------------------------------------------------#
        # This comes from the user
        #----------------------------------------------------------------#
        if action:
            if action == "KILL":
                globalParams["STATE"] = States.FINISH
            elif action == "STOP":
                globalParams["STATE"] = States.FORCEWAIT
            elif action == "RESUME":
                globalParams["STATE"] = States.RESUME
            elif action == "SWITCHQ":
                queue = request.forms.get('QUEUE')
                queueLockT.acquire()
                for job in qToSubmit:
                    job.queue = queue
                queueLockT.release()
                query = list(db.jobs.find({  # we want only jobs at our site
                    u'user': user,
                    u'task': {u'$in': [x.name for x in tasks]},
                    u'from': hostname,
                    u'status': u'Pending'
                }, {u'hash': 1, u'subtask': 1, u'_id': 0}))
                for entry in query:
                    job = FindJobFromHash(entry)
                    if job:
                        job.SwitchToQueue(queue)
                returnString = "Operation succeeded\n"
                pass
            elif action == "CHANGEMAXJ":
                globalParams["userlimit"] = int(request.forms.get('MAXJOBS'))
                logging.info("Maxjobs changed to {}".format(
                    globalParams["userlimit"]))
                returnString = "Operation succeeded\n"
                pass
            elif action == "CHECK":
                returnString = """{} jobs to be submitted
{} jobs pending
{} jobs running
""".format(len(qToSubmit), globalParams["nPending"], globalParams["nRunning"])

        logging.debug(States.NAMES[globalParams["STATE"]])
        try:
            return returnString
        except socket.error, e:
            print "errno is %d" % e[0]

    testPort = "test"
    while True:
        testPortCmd = "netstat -an | grep "
        if platform.system() == "Darwin":
            testPortCmd += "." + str(hostport)
        else:
            testPortCmd += ":" + str(hostport)

        logging.debug(testPortCmd)
        testPort = subprocess.Popen(
            [testPortCmd, ''], shell=True, stdout=subprocess.PIPE).stdout.read().strip()
        logging.debug(testPort)
        if testPort == "":
            logging.info("Port " + str(hostport) + " is free")
            break
        else:
            logging.info("Port " + str(hostport) + " is used")
            hostport += 1
            pass
        sys.stderr.flush()

    run(host=hostname, port=hostport)
    logging.debug('exit')


def HandleFailedJobs():
    nFailed = 0

    failedJobs = []

    while True:
        if globalParams["STATE"] == States.FINISH:
            logging.debug("Exiting...")
            return

        logging.debug("HandleFailedJobs")

        # take everything
        logging.debug("Querying DB...")
        tStart = time.time()
        query = list(db.jobs.find({u'task': {u'$in': [x.name for x in tasks]}, u'status': u'Error'}, {
            u'subtask': 1, u'status': 1, u'hash': 1, u'retries': 1, u'_id': 0}))
        tEnd = time.time()
        logging.debug("Found {} results".format(len(query)))
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        #----------------------------------------------------------------#
        # Query for failed jobs and re-queue them
        #----------------------------------------------------------------#
        logging.debug("Looking for failed jobs...")
        tStart = time.time()
        # pool = Pool(processes=8)
        failedJobs = [x for x in query
                      if (
                          x[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                          True)
                      ]
        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        nFailed = len(failedJobs)
        if nFailed > 0:
            logging.debug("Found " + str(nFailed) + " failed jobs in DB")

        # for entry in [x for x in failedJobs]:
        def DispatchFailedJob(entry):
            job = FindJobFromHash(entry)
            if not job:
                logging.debug("Job not found in memory")
                return
            if job.queued:
                logging.debug("Job already queued")
                return
            # job.Dump()
            job.retries = int(entry['retries'])
            # db.jobs.delete_one({'_id' : entry['_id']})
            if job.retries < 3:
                job.queued = True
                logging.debug("Queueing Job")
                # if we submit it soon it a check less in this function later
                qToSubmit.appendleft(job)
            else:
                logging.info(
                    "Failed processing for file {}".format(job.outfile))

        logging.debug("Handling failed jobs...")
        tStart = time.time()
        for _job in failedJobs:
            DispatchFailedJob(_job)
        # pool.map_async(DispatchFailedJob, failedJobs)
        # pool.close()
        # pool.join()
        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        logging.debug("Going to sleep...")
        time.sleep(300)


def BackFillFromDB():
    global globalParams
    global isBackFillReady
    global BackFillResultUsed

    isBackFillReady = False

    jobType = "HTCondorJob" if batchSystem == "HTCondor" else "LSFJob"

    while True:
        if globalParams["STATE"] == States.FINISH:
            logging.debug("Exiting...")
            return

        logging.debug("BackFill")

        # take everything
        logging.debug("Querying DB...")
        tStart = time.time()
        query = db.jobs.find({u'task': {u'$in': [x.name for x in tasks]}, u'status': {u'$ne': u'Done'}
                              }, {u'status': 1, u'subtask': 1, u'user': 1, u'from': 1, u'type': 1, u'_id': 0})
        jobList = list(query)
        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        for task in tasks:
            task.Update()

        global _nPending
        global _nRunning
        global _nPendingTot
        global _nRunningTot
        _nPending, _nRunning, _nPendingTot, _nRunningTot = Value(
            'i', 0), Value('i', 0), Value('i', 0), Value('i', 0)

        def InitArgs(__nPending, __nRunning, __nPendingTot, __nRunningTot):
            global _nPending
            global _nRunning
            global _nPendingTot
            global _nRunningTot
            _nPending, _nRunning, _nPendingTot, _nRunningTot = __nPending, __nRunning, __nPendingTot, __nRunningTot

        pool = Pool(processes=8, initializer=InitArgs, initargs=(
            _nPending, _nRunning, _nPendingTot, _nRunningTot,))

        logging.debug("Accumulating results...")
        tStart = time.time()

        # don't run expensive queries on the DB. Do the math locally...
        # for job in query:

        def HandleQueryResult(job):
            global _nPending
            global _nRunning
            global _nPendingTot
            global _nRunningTot
            # print job[u'hash'], job[u'status']
            if job[u'status'] == u'Pending':
                with _nPendingTot.get_lock():
                    _nPendingTot.value += 1

                if (
                    # job[u'task'] in [t.name for t in tasks] and
                    job[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                    job[u'user'] == user and
                    job[u'from'] == hostname and
                    job[u'type'] == jobType and
                        True):
                    with _nPending.get_lock():
                        _nPending.value += 1
            elif job[u'status'] == u'Running':
                with _nRunningTot.get_lock():
                    _nRunningTot.value += 1

                if (
                    # job[u'task'] in [t.name for t in tasks] and
                    job[u'subtask'] in [s.name for t in tasks for s in t.subtasks] and
                    job[u'user'] == user and
                    job[u'from'] == hostname and
                    job[u'type'] == jobType and
                        True):
                    with _nRunning.get_lock():
                        _nRunning.value += 1

        pool.map_async(HandleQueryResult, jobList, chunksize=1)
        pool.close()
        pool.join()
        globalParams["nPending"], globalParams["nRunning"], globalParams["nPendingTot"], globalParams[
            "nRunningTot"] = _nPending.value, _nRunning.value, _nPendingTot.value, _nRunningTot.value
        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        # for thread in updateThr:
        #     thread.join()

        if not isBackFillReady:
            if globalParams["limit"] == 0:
                globalParams["limit"] = globalParams["nPending"] + \
                    globalParams["nRunning"] + 2
            isBackFillReady = True

        BackFillResultUsed = False

        PrintSummary()
        time.sleep(3)


def CleanupMemory():
    global qRemoved
    global isBackFillReady

    time.sleep(10)  # give time to BackFill to run at least once
    while not isBackFillReady:
        time.sleep(1)

    lastQueryTime = datetime.datetime(1970, 1, 1)

    while True:
        if globalParams["STATE"] == States.FINISH:
            logging.debug("Exiting...")
            return

        logging.debug("Querying DB...")
        tStart = time.time()
        query = db.jobs.find({u'task': {u'$in': [x.name for x in tasks]}, u'status': u'Done',
                              u'subtask': {u'$in': [y.name for x in tasks for y in x.subtasks]}}, {u'hash': 1, u'task': 1, u'subtask': 1, u'finishTime': 1, u'_id': 0})

        doneJobs = list(query)
        thisQueryTime = datetime.datetime.now()

        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        pool = Pool(processes=4)

        def HandleQueryResult(job):
            if (not u'finishTime' in job) or job[u'finishTime'] > lastQueryTime:
                # if not job[u'hash'] in qRemoved:
                RemoveFinishedJob(job)
                qRemoved.append(job[u'hash'])
                # logging.debug("Job {} removed from memory".format(job[u'hash']))

        logging.debug("Cleaning memory...")
        tStart = time.time()
        pool.map_async(HandleQueryResult, doneJobs, chunksize=1)
        pool.close()
        pool.join()
        tEnd = time.time()
        logging.debug("took {:6.2f}s".format(tEnd - tStart))

        lastQueryTime = thisQueryTime
        time.sleep(600)


def main():
    signal.signal(signal.SIGINT, SigHandler)

    print "-"*80
    print "\n", "- Job launcher for AMSPG Ions group", "\n"
    print "-"*80

    parser = argparse.ArgumentParser(
        description='Job launcher for PG nuclei analysis.')
    parser.add_argument('-f', '--file', type=str, nargs='+',
                        help='YAML file with task definition')
    parser.add_argument('-k', '--keytab', type=str,
                        nargs=1, help='Keytab file')
    parser.add_argument('-b', '--batch', type=str, nargs=1,
                        help='Batch system (LSF / HTCondor)')
    parser.add_argument('-u', '--user', type=str, nargs=1,
                        help='Username to be used for accounting')
    parser.add_argument('-w', '--overwrite', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('-c', '--clean_tasks', action='store_true',
                        help='Clean specified tasks from DB before launching')
    parser.add_argument('--hadd', action='store_true',
                        help='Specify if hadd is required after execution')
    parser.add_argument('-q', '--queue', type=str, nargs=1,
                        help='The queue to run on')
    parser.add_argument('-p', '--port', type=str, nargs=1,
                        help='HTTP port to use (default=8080)')
    parser.add_argument('-H', '--dbhost', type=str, nargs=1,
                        help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-U', '--dbuser', type=str, nargs=1,
                        help='MongoDB user. Mandatory')
    parser.add_argument('-P', '--dbpass', type=str, nargs=1,
                        help='MongoDB password. Mandatory')
    parser.add_argument('--condorname', type=str, nargs=1,
                        help='HTCondor schedd name')
    parser.add_argument('--condorpool', type=str, nargs=1,
                        help='HTCondor schedd pool')
    parser.add_argument('--vo', type=str, nargs=1,
                        help='VO to use for x509 proxy generation')
    parser.add_argument('--vomsdir', type=str, nargs='*',
                        help='Path to the vomsdir (optional)')
    parser.add_argument('-bg', '--background', action='store_true',
                        help='Send process in the background (daemonize)')
    parser.add_argument('-t', '--test', action='store_true',
                        help='Test (don\'t submit any job...)')
    parser.add_argument('-g', '--debug', action='store_true',
                        help='Show debug info')

    global args
    args = parser.parse_args()

    global tasks
    tasks = []
    SetupInputPar()

    global db, dbMgr, dbhost, dbuser, dbpass

    dbMgr = MongoDBManager(dbhost, "IonsFluxes", dbuser, dbpass)
    dbMgr.Connect()

    db = dbMgr.db

    for taskfile in taskfiles:
        tasks.append(GetYamlParams(taskfile))

    GetFilelists()

    GetOutDir()

    #----------------------------------------------------------------#
    # Check if daemon mode is requested
    #----------------------------------------------------------------#
    mainlogfile = os.environ["PWD"] + "/launch_jobs.log"
    if args.background:
        logging.info("Going in daemon mode...")

        daemonize()

        logging.info("Logging in {}".format(mainlogfile))
        mainlog = open(mainlogfile, 'w')
        logging.basicConfig(level=logLevel,
                            filename=mainlogfile,
                            format='[%(levelname)s] (%(threadName)s) %(message)s',
                            )
        sys.stdout = mainlog
        sys.stderr = mainlog

        pass

    global user
    if not user:
        user = subprocess.Popen(["whoami", ''], shell=True,
                            stdout=subprocess.PIPE).stdout.read().strip()

    sys.stdout.flush()

    # All the stuff now happens here
    # acquire a lock to block port number
    # global portLock
    # portLock = threading.Lock()

    # acquire a lock to block queue manipulation
    # to allow indipendent queue edit setup a lock for each queue
    global queueLockT
    queueLockT = threading.Lock()

    # portLock.acquire()
    # start HTTP listener on a separate thread
    threads = []
    #lsn = threading.Thread(name='HTTP Listener', target=Listener)
    lsn = Process(name='HTTP Listener', target=Listener, args=(globalParams,))
    threads.append(lsn)

    chkTarget = None
    if batchSystem == "LSF":
        chkTarget = CheckLSFStatus
    elif batchSystem == "HTCondor":
        chkTarget = CheckHTCondorStatus

    chk = threading.Thread(name='Job Checker', target=chkTarget)
    chk.setDaemon(True)
    threads.append(chk)

    bckfill = threading.Thread(name='BackFiller', target=BackFillFromDB)
    bckfill.setDaemon(True)
    threads.append(bckfill)

    hfj = threading.Thread(name='HandleFailedJobs', target=HandleFailedJobs)
    hfj.setDaemon(True)
    threads.append(hfj)

    cleanup = threading.Thread(name='CleanupMemory', target=CleanupMemory)
    cleanup.setDaemon(True)
    threads.append(cleanup)

    global queueIsDone
    queueIsDone = False
    queuing = threading.Thread(name='Queueing', target=QueueJobs)
    queuing.setDaemon(True)
    threads.append(queuing)

    # Prepare job queues
    sys.stdout.flush()

    lsn.start()
    queuing.start()

    global virtOrg
    if virtOrg:
        proxyrenew = threading.Thread(
            name='ProxyRenewal', target=RenewX509Proxy)
        proxyrenew.setDaemon(True)
        threads.append(proxyrenew)
        proxyrenew.start()

    bckfill.start()
    PrintSummary()

    hfj.start()
    cleanup.start()

    chk.start()

    subMgr = SubmissionManager(8, 8, threads)

    # run machine
    FiniteStateMachine(subMgr)
    lsn.terminate()
    lsn.join()

    logging.info("Exiting...")
    subMgr.queue.join()
    sys.exit(0)
    logging.info("!")


if __name__ == "__main__":

    main()
