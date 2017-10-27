import os
import sys
import signal
import re
import time
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

from cStringIO import StringIO
from Queue import Queue
from job.SubTask import SubTask
from job.LSFJob import LSFJob
from job.HTCondorJob import HTCondorJob
from dbmgr import  MongoDBManager
from bottle import route, run, post, request, response
from itertools import (takewhile,repeat)

scriptdir = os.path.dirname(os.path.realpath(__file__))
UMASK = 0
WORKDIR=scriptdir

qToSubmit = collections.deque()
nPending = 0
nRunning = 0
limit = 0

class States:
    SUBMIT, WAIT, FORCEWAIT, RESUME, REDUCE, FINISH = range(6)
    NAMES = ["SUBMIT", "WAIT", "FORCEWAIT", "RESUME", "REDUCE", "FINISH"]

STATE = States.WAIT
subTasks = []

def daemonize():
    #Daemon-izing the process. Look here for more details: http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
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
            #os.chdir(WORKDIR)
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
    return sum( buf.count(b'\n') for buf in bufgen )

#----------------------------------------------------------------#
# Code to find filelists for datasets
#----------------------------------------------------------------#
def GetFilelists( datasets, wdir ):
    isMC = []
    list = []

    if (not wdir.endswith('/') and len(wdir) > 0):
        wdir += '/'
        pass

    for dataset in datasets:
        if "pass" in dataset:
            isMC.append( False )
            print "Getting filelists for", dataset
            list.append( sorted(glob.glob( wdir + (".filelists/*%s.txt" % dataset) )) )
            print ' - %s \n' % ', '.join(map(str, list[-1]))
            pass
        else:
            #TODO: per i dati sembra andare piu' o meno ok, per il MC ci sara' da lavorare...
            isMC.append( True )
            mcbuildlist = dataset.split('.')
            mcbuildlist[0:2] = ['.'.join(mcbuildlist[0:2])]
            if len(mcbuildlist) == 1:
                print "Getting runlists for MC production", mcbuildlist[0]
                list.append( sorted(glob.glob( wdir + (".filelists/%s/*.txt" % mcbuildlist[0]) )) )
                pass
            else:
                print "Getting runlists for MC production", mcbuildlist[0], "tuning", mcbuildlist[1]
                list.append( sorted(glob.glob( wdir + ( ".filelists/%s/*%s.txt" % (mcbuildlist[0], '.'.join(mcbuildlist[1:])) ) )) )
                pass
            print ' - %s \n' % '\n - '.join(map(str, list[-1]))

    return isMC, list


def ParseExecArgs(args):
    arglist = []
    for arg in args:
        temp = arg.split('=')
        if len(temp) == 1:
            arglist.append("-" + temp[0])
            pass
        elif len(temp) == 2:
            arglist.append("-" + temp[0] + temp[1])
            pass
        else:
            print "Error: Invalid argument", arg
            pass
    argstring = str( ' '.join(arglist[:]) )

    return argstring


#----------------------------------------------------------------#
# Code to get outdir names
#----------------------------------------------------------------#
def GetOutDir( outdir, datasets, filelists ):
    outlist = []
    if not outdir.startswith("/"):
        outdir = cwd + '/' + outdir
        pass

    print "Setting output directories"
    for i in range(len(datasets)):
        if "pass" in datasets[i]:
            print " -", str(outdir+'/'+datasets[i])
            outlist.append( [str(outdir+'/'+datasets[i])] )
            pass
        else:
            mcbuildlist = datasets[i].split('.')
            mcbuildlist[0:2] = ['.'.join(mcbuildlist[0:2])]
            outlist.append( [] )
            for j in range(len(filelists[i])):
                result1 = re.search(str(mcbuildlist[0]), str(filelists[i][j]))
                result2 = re.search('\.txt', str(filelists[i][j]))
                string = filelists[i][j][result1.end()+1:result2.start()]
                print " -", str(outdir+'/'+datasets[i]+'/'+string)
                outlist[-1].append( str(outdir+'/'+datasets[i]+'/'+string) )
            pass

    print "\n"
    return outlist

#----------------------------------------------------------------#
# Code to setup global variables
#----------------------------------------------------------------#
def SetupInputPar():
    global hostname
    hostname = str(subprocess.Popen(['hostname',''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip())

    global useCWD
    useCWD = True

    #----------------------------------------------------------------#
    # SETUP task name, executable and working dir
    #----------------------------------------------------------------#
    if not args.task:
        print "Error: you need to provide name for the task (-T option)"
        sys.exit(1)
        pass

    if not args.exe:
        print "Error: you need to provide an executable (-e option)"
        sys.exit(1)
        pass

    if not args.cwd:
        print "Error: you need to specify working directory (-c option)"
        sys.exit(1)
        pass

    global task
    task = str( args.task[0] )

    global exe
    exe = str( args.exe[0] )

    global cwd
    cwd = os.path.abspath( str( args.cwd[0] ) )
    if not cwd.endswith('/'):
        cwd += "/"
        pass

    if not os.path.isabs( exe ):
        exe = os.path.abspath( exe )

    #----------------------------------------------------------------#
    # Get MongoDB host, user and pass
    #----------------------------------------------------------------#
    if not args.dbuser:
        print "Error: you need to provide a user to log on the DB (-U option)"
        sys.exit(1)
        pass

    if not args.dbpass:
        print "Error: you need to provide a password to log on the DB (-P option)"
        sys.exit(1)
        pass

    global dbuser
    dbuser = str( args.dbuser[0] )

    global dbpass
    dbpass = str( args.dbpass[0] )

    global dbhost
    if args.dbhost:
        dbhost = str( args.dbhost[0] )
    else:
        dbhost = hostname


    #----------------------------------------------------------------#
    # Check if daemon mode is requested
    #----------------------------------------------------------------#
    logLevel = logging.INFO
    if args.debug:
        logLevel = logging.DEBUG
    mainlogfile = cwd + "/launch_jobs.log"
    if args.background:
        print  "Going in daemon mode..."

        daemonize()

        print "Logging in", mainlogfile
        mainlog = open(mainlogfile,'w')
        logging.basicConfig(level=logLevel,
                            filename=mainlogfile,
                            format='[%(levelname)s] (%(threadName)s) %(message)s',
                            )
        sys.stdout = mainlog
        sys.stderr = mainlog

        pass
    else:
        logging.basicConfig(level=logLevel,
                            format='[%(levelname)s] (%(threadName)s) %(message)s',
                            )


    print "-"*80
    print "\n", "- Job launcher for PG Ions analysis", "\n"
    print "-"*80


    #----------------------------------------------------------------#
    # Get datasets
    #----------------------------------------------------------------#
    if not args.dataset:
        print "Error: no dataset specified (-d option)"
        sys.exit(1)
        pass
    global datasets
    datasets = args.dataset
    print "Requested datasets:"
    print ' - %s \n' % ', '.join(map(str, datasets))


    #----------------------------------------------------------------#
    # Check if executable exists
    #----------------------------------------------------------------#
    if not os.path.isfile( exe ):
        print "Error: Executable", exe, "not found"
        sys.exit(1)
    print "Executable to run:", exe, "\n"


    #----------------------------------------------------------------#
    # Get cl arguments for the executable
    #----------------------------------------------------------------#
    global argstring_data, argstring_mc
    argstring_data = ""
    argstring_mc   = ""
    if args.exeargsdata:
        argstring_data = ParseExecArgs( args.exeargsdata )
        pass
    if args.exeargsmc:
        argstring_mc = ParseExecArgs( args.exeargsmc )
        pass

    if args.overwrite:
        argstring_mc   += " -w"
        argstring_data += " -w"
        pass


    #----------------------------------------------------------------#
    # Get amsvar
    #----------------------------------------------------------------#
    if not args.amsvar:
        print "Error: please provide amsvar script (-a option)"
        sys.exit(1)
        pass

    global amsvar
    amsvar = os.path.abspath( str(args.amsvar[0]) )
    if not os.path.isfile( amsvar ):
        print "Error: amsvar script not found"
        sys.exit(1)
        pass
    # else:
    #     amsvar = os.path.abspath( str(args.amsvar[0]) )

    # if not amsvar.startswith('/'):
    #     amsvar = os.getcwd() + '/' + amsvar
    #     pass

    print "Using amsvar script:", amsvar

    #----------------------------------------------------------------#
    # Get batch system
    #----------------------------------------------------------------#
    if not args.batch:
        print "Error: no batch specified"
        sys.exit(1)
        pass
    global batchSystem
    if args.batch[0] == "lsf" :
        batchSystem = "LSF"
    elif args.batch[0] == "condor":
        batchSystem = "HTCondor"
    else:
        print "Batch system not specified or not supported"
        sys.exit(1)

    print "Using", batchSystem, "\n"


    #----------------------------------------------------------------#
    # Get queue
    #----------------------------------------------------------------#
    if not args.queue:
        print "Error: no queue specified"
        sys.exit(1)
        pass
    global queue
    queue = args.queue[0]
    print "Submitting on queue", queue, "\n"

    #----------------------------------------------------------------#
    # Check reduce
    #----------------------------------------------------------------#
    if args.hadd:
        print "With hadd step"
        pass

    #----------------------------------------------------------------#
    # Get indir and outdir
    #----------------------------------------------------------------#
    if not args.indir:
        print "Error: no input directory specified"
        sys.exit(1)
        pass

    if not args.outdir:
        print "Error: no output directory specified"
        sys.exit(1)
        pass

    global indir
    indir = args.indir[0]
    if not indir.endswith('/'):
        indir += '/'

    global outdir
    outdir = args.outdir[0]

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
    # Get maxjobs
    #----------------------------------------------------------------#
    global limit
    if not args.maxjobs:
        limit = 1000
        pass
    else:
        limit = int(args.maxjobs[0])

def SigHandler(signum, frame):
    print 'Signal handler called with signal', signum
    if signum in [signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGKILL]:
        sys.exit(1)
        pass


def QueueJobs( filelist, outlist, argstring, queue, dataset ):
    logging.info("Preparing jobs for submission...")

    for iodir in ["pin", "perr", "pout"]:
        if (not args.test) and (not os.path.exists( cwd+iodir )):
            os.makedirs( cwd+iodir )
            os.chmod( cwd+iodir, 0775 )
            pass
        pass

    global jobscript
    jobscript = scriptdir+'/'+"job.sh"
    if not os.path.isfile( jobscript ):
        print "Error: job.sh not found ( looking in", scriptdir, ")"
        #sys.exit(2)
        pass


    subTasks.append( SubTask( dataset, task ) )
    icount = 0
    totfiles = 0
    for ilist in range(0, len(filelist)):
        totfiles += rawincount( filelist[ilist] )
    subTasks[-1].totJobs = totfiles

    for ilist in range(0, len(filelist)):

        print "\nSubmitting jobs for filelist", filelist[ilist], "jobs on queue", queue, "\n"
        file_flist = open(filelist[ilist], 'r')
        odir  = outlist[ilist]

        if (not args.test) and (not os.path.exists( odir )):
            print "Creating output dir", odir
            os.makedirs( odir )
            os.chmod( odir+"/..", 0775 )
            os.chmod( odir, 0775 )
            pass
        pass

        old_perc_progress = 0

        outfilelist = []

        portLock.acquire()
        for ifile in file_flist:
            sys.stdout.flush()
            icount += 1

            perc_progress = math.floor(100*icount/totfiles)
            if perc_progress != old_perc_progress:
                sys.stdout.write("Loading filelist... %d%%   \r" % (perc_progress) )
                sys.stdout.flush()
                old_perc_progress = perc_progress


            if not os.path.isabs( ifile.strip() ):
                ifile = cwd + ifile
            #----------------------------------------------------------------#
            # Get outfile
            #----------------------------------------------------------------#
            ofile = str( os.path.basename( ifile.strip() ) )
            if ".txt" in ofile:
                ofile = ofile.replace(".txt", ".root")
                pass

            if batchSystem == "LSF":
                job = LSFJob( icount, subTasks[-1] )
            elif batchSystem == "HTCondor":
                job = HTCondorJob( icount, subTasks[-1] )

            job.useCWD = useCWD

            job.amsvar     = amsvar
            job.exe        = exe
            job.workdir    = cwd
            job.arguments  = argstring
            job.indir      = indir
            job.outdir     = odir
            job.infile     = ifile.strip()
            job.outfile    = ofile
            job.queue      = queue
            job.flags     += dataset
            job.script     = jobscript
            job.user       = user
            job.site       = hostname

            job.FillVars()
            job.SetName()
            job.GetCMD()
            # job.Dump()

            subTasks[-1].jobList.append( job )

            #If we don't overwrite and the output file is already present
            #We insert the job in the DB as Done
            if not args.overwrite:
                if os.path.isfile( odir + '/' + ofile ):
                    query = db.jobs.find_one({ u'hash' : job.hash, u'task' : job.task })
                    if query:
                        if query['status'] == 'Done':
                            continue
                        else:
                            logging.error("Output file {} present but job not in Done state. Please check!!".format(odir + '/' + ofile))
                            continue
                    else:
                        dbMgr.InsertJob( job, u'Done', True )
                        continue
                    pass
                pass

            #----------------------------------------------------------------#
            # Check if job is already in DB
            #----------------------------------------------------------------#
            query = db.jobs.find_one({ u'hash' : job.hash, u'task' : job.task })
            if query:
                print "Job", str(icount) + "/" + str(totfiles), "already submitted. Skipping"
                continue

            job.queued = True
            qToSubmit.append( job )


        #prep command for reduce step
        subTasks[-1].postCmd  = "bsub -q " + queue
        subTasks[-1].postCmd += " -oo " + cwd + "/pout/hadd_" + dataset + "_" + os.path.basename(odir+".txt") + " "
        subTasks[-1].postCmd += "\"hadd -f " + odir+".root" + " " + odir + "/*\""

        portLock.release()


def PrintSummary():
    logging.info( str(len(qToSubmit)) + " jobs to be submitted")
    logging.info( str(nPending) + " jobs pending")
    logging.info( str(nRunning) + " jobs running")
    sys.stderr.flush()


def FindJobFromHash( jobID ):
    for index in range(0, len(subTasks)):
        jobSearch = [x for x in subTasks[index].jobList if x.hash == jobID]
        if len(jobSearch) > 0:
            return jobSearch[0]
        else:
            return None


#----------------------------------------------------------------#
# Atomic action to submit one reduce action
#----------------------------------------------------------------#
def SubmitReduce(cmd):
    logging.info("Submitting reduce job")
    sys.stderr.flush()
    if (not args.test):
        #logging.debug(cmd)
        try:
            dummy = subprocess.Popen([cmd,''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip()
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
def SubmitJob():

    # get next job
    queueLockT.acquire()
    job = qToSubmit.popleft()
    job.queued = False
    queueLockT.release()

    jobIsRetrying = False

    #----------------------------------------------------------------#
    # Check if job is already in DB
    #----------------------------------------------------------------#
    query = db.jobs.find_one({ u'hash' : job.hash, u'task' : job.task })
    if query:
        if query['status'] in ['Pending', 'Running']:
            print "Job", str(job.i) + "/" + str(job.totfiles), "already submitted (" + str(query['user']) + " on host " + str(query['from']) + "). Skipping"
            return
        elif query['status'] == 'Done':
            if not args.overwrite:
                print "Job", str(job.i) + "/" + str(job.totfiles), "already done (" + str(query['user']) + " on host " + str(query['from']) + "). Skipping"
                return
        elif query['status'] == 'Error':
            job.retries = query['retries'] + 1
            jobIsRetrying = True


    if (not args.test):
        #----------------------------------------------------------------#
        # Submit the job
        #----------------------------------------------------------------#
        print "Submitting job", str(job.i) + "/" + str(job.totfiles)
        sys.stdout.flush()
        insertStatus = dbMgr.InsertJob( job, u'Pending', True if jobIsRetrying else False )
        if insertStatus == False:
            return
        subStatus = job.Submit()
        logging.debug("Submit status: " + str(subStatus))
        if not dbMgr.UpdateJobWithID( job, subStatus ):
            return

        print "Job", str(job.i) + "/" + str(job.totfiles), "submitted"
        sys.stdout.flush()
        pass
    else:
        print "submitting one job", job.hash
        print job.cmd + "\n"
        dbMgr.InsertJob( job, u'Pending', True if jobIsRetrying else False )
        pass


#----------------------------------------------------------------#
# Manual check of running jobs still being there (LSF version)
#----------------------------------------------------------------#
def CheckLSFStatus():
    while True:
        if nRunning < 1 and nPending < 1:
            time.sleep(0.1)
            continue

        time.sleep(10)    # wait a few of seconds, give time to LSF to
                          # update the status of the jobs...
        logging.info("Checking LSF status...")
        bjcmd = subprocess.Popen(["bjobs",''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        bjout = bjcmd.stdout.read().strip()
        bjerr = bjcmd.stderr.read().strip()
        bjout = "\n".join( bjout.splitlines()[1:] ) # remove first line (LSF header)
        logging.debug("...done")
        logging.debug( "out {} lines, err {} lines".format( len(bjout.splitlines()), len(bjerr.splitlines()) ) )

        templistP = list( db.jobs.find({
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : 'Pending'
        }) )

        templistR = list( db.jobs.find({
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : 'Running'
        }) )

        #parse running jobs
        logging.info("Parsing running jobs...")
        print len(templistR), "running jobs in cache"
        for entry in templistR:
            temp = [ x.split() for x in bjout.splitlines() if int(x.split(' ')[0]) == int(entry['lsfID']) ]
            #print "Searching", entry['lsfID'], "({})".format(type(entry['lsfID'])), "=", temp
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                logging.debug( "job " + str(entry['lsfID']) + " not in running state but it should" )
                job = FindJobFromHash( entry['hash'] )
                db.jobs.delete_one({'_id' : entry['_id']})

                if job:
                    queueLockT.acquire()
                    qToSubmit.append( job )
                    queueLockT.release()
                pass
            elif line[2] == "RUN":
                logging.debug( str(entry['lsfID']) + " is in RUN state" )
                continue

        #parse pending jobs
        logging.info("Parsing pending jobs...")
        print len(templistR), "running jobs in cache"
        for entry in templistP:
            temp = [ x.split() for x in bjout.splitlines() if int(x.split(' ')[0]) == int(entry['lsfID']) ]
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                logging.debug( "job " + str(entry['lsfID']) + " not in pending state but it should" )
                job = FindJobFromHash( entry['hash'] )

                if job and job.notFoundPending:
                    db.jobs.delete_one({'_id' : entry['_id']})
                    queueLockT.acquire()
                    qToSubmit.append( job )
                    queueLockT.release()
                elif job: # for this time just mark it, eventually we will act in a couple of minutes
                    job.notFoundPending = True
                    pass
            elif line[2] == "PEND":
                logging.debug( str(entry['lsfID']) + " is in PEND state" ) ## not printed??
                continue
            elif line[2] == "RUN":
                logging.debug( "job " + str(entry['lsfID']) + " in running state but it should be pending" )

        logging.info("...going to sleep")
        time.sleep(600) # wait 10 minutes


#----------------------------------------------------------------#
# Manual check of running jobs still being there (HTCondor version)
# TODO: capire come va implementata
#----------------------------------------------------------------#
def CheckHTCondorStatus():
    return

    ##postpone test for later...
    while True:
        if nRunning < 1 and nPending < 1:
            time.sleep(0.1)
            continue

        time.sleep(10)    # wait a few of seconds, give time to LSF to
                          # update the status of the jobs...
        logging.info("Checking HTCondor status...")
        bjout = subprocess.Popen(["condor_q -nobatch",''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip()
        bjout = "\n".join( bjout.splitlines()[4:] ) # remove first 4 lines (HTCondor header)
        logging.debug("...done")

        templistP = list( db.jobs.find({
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : 'Pending'
        }) )

        templistR = list( db.jobs.find({
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : 'Running'
        }) )

        #parse running jobs
        logging.info("Parsing running jobs...")
        for entry in templistR:
            temp = [ x.split() for x in bjout.splitlines() if int(x.split(' ')[0]) == entry['lsfID'] ]
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                logging.debug( "job " + str(entry['lsfID']) + " not in running state but it should" )
                job = FindJobFromHash( entry['hash'] )
                db.jobs.delete_one({'_id' : entry['_id']})

                if job:
                    queueLockT.acquire()
                    qToSubmit.append( job )
                    queueLockT.release()
                pass
            elif line[2] == "RUN":
                logging.debug( str(entry['lsfID']) + " is in RUN state" )
                continue

        #parse pending jobs
        logging.info("Parsing pending jobs...")
        for entry in templistP:
            temp = [ x.split() for x in bjout.splitlines() if int(x.split(' ')[0]) == entry['lsfID'] ]
            if len(temp) > 0:
                line = temp[0]
            else:
                line = []

            if len(line) < 1:
                logging.debug( "job " + str(entry['lsfID']) + " not in pending state but it should" )
                job = FindJobFromHash( entry['hash'] )

                if job and job.notFoundPending:
                    db.jobs.delete_one({'_id' : entry['_id']})
                    queueLockT.acquire()
                    qToSubmit.append( job )
                    queueLockT.release()
                elif job: # for this time just mark it, eventually we will act in a couple of minutes
                    job.notFoundPending = True
                    pass
            elif line[2] == "PEND":
                logging.debug( str(entry['lsfID']) + " is in PEND state" ) ## not printed??
                continue
            elif line[2] == "RUN":
                logging.debug( "job " + str(entry['lsfID']) + " in running state but it should be pending" )

        logging.info("...going to sleep")
        time.sleep(600) # wait 10 minutes

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
def FiniteStateMachine():
    logging.debug('start')

    global qToSubmit
    global nPending
    global nRunning
    global limit

    global STATE
    STATE = States.WAIT
    prevSTATE = STATE

    time.sleep(2) #give time to BackFill to run at least once

    while True:

        #----------------------------------------------------------------#
        # Check if we have a closed subtask
        #----------------------------------------------------------------#
        isTaskDone = True
        readyReduce = -1
        for subtask in subTasks:
            isTaskDone = isTaskDone and subtask.IsDone()
            if subtask.IsDone() == True:
                readyReduce = subTasks.index(subtask)
                break
            pass

        jobsSubmitted = nPending + nRunning

        #----------------------------------------------------------------#
        # State switch rules
        #----------------------------------------------------------------#
        if STATE == States.FORCEWAIT:
            time.sleep(0.1)
            continue

        queueLockT.acquire() #block queue to determine state
        if readyReduce > -1 and args.hadd:
            logging.debug("Reduce step to be performed, set: " + str(readyReduce))
            STATE = States.REDUCE
        elif STATE == States.FINISH:
            logging.info('Finished')
            queueLockT.release()
            # if isTaskDone:
            #     MongoDBManager().CleanTaskFromDB( task )
            return
            pass
        elif (len(qToSubmit) + nRunning + nPending == 0):
            STATE = States.FINISH
        elif jobsSubmitted >= limit:
            if(STATE == States.SUBMIT):     #Print summary when stopping submit
                PrintSummary()
                logging.debug('limit at ' + str(limit))
            STATE = States.WAIT
        elif len(qToSubmit) > 0:
            STATE = States.SUBMIT
        else:
            if prevSTATE == States.SUBMIT:
                PrintSummary()
            STATE = States.WAIT
        queueLockT.release()
        newSTATE = STATE

        if args.debug:
            if newSTATE != prevSTATE:
                logging.debug( "Going from " + States.NAMES[prevSTATE] + " to " + States.NAMES[newSTATE] )

        #----------------------------------------------------------------#
        # Map state to action
        #----------------------------------------------------------------#
        if STATE == States.WAIT:
            time.sleep(1)
        elif STATE == States.SUBMIT:
            SubmitJob()
        elif STATE == States.RESUME:
            pass                         # Just go to the next tick
        elif STATE == States.REDUCE:
            logging.debug("We are in REDUCE state, set " + str(readyReduce))
            SubmitReduce( subTasks[readyReduce].postCmd )
            subTasks.remove( subTasks[readyReduce] )

        prevSTATE = STATE
        pass

        time.sleep(0.01)

#----------------------------------------------------------------#
# HTTP listener daemon
#----------------------------------------------------------------#
def Listener():
    logging.debug('start')
    global hostport

    @post('/process')
    def handle_request():
        global limit
        global STATE
        action = request.forms.get('action')
        status = request.forms.get('status')

        returnString = ""

        #----------------------------------------------------------------#
        # This comes from the user
        #----------------------------------------------------------------#
        if action:
            if action == "KILL":
                STATE = States.FINISH
            elif action == "STOP":
                STATE = States.FORCEWAIT
            elif action == "RESUME":
                STATE = States.RESUME
            elif action == "SWITCHQ":
                queue = request.forms.get('QUEUE')
                queueLockT.acquire()
                for job in qToSubmit:
                    job.queue = queue
                queueLockT.release()
                query = list(db.jobs.find({ #we want only jobs at our site
                u'user' : user,
                u'task' : task,
                u'from' : hostname,
                u'status' : u'Pending'
                }))
                for item in query:
                    job = FindJobFromHash( item['hash'] )
                    job.SwitchToQueue(queue)
                returnString = "Operation succeeded\n"
                pass
            elif action == "CHANGEMAXJ":
                limit = int(request.forms.get('MAXJOBS'))
                logging.info("Maxjobs changed to {}".format(limit))
                returnString = "Operation succeeded\n"
                pass
            elif action == "CHPEND":
                queueLockP.acquire()
                old_stdout = sys.stdout
                sys.stdout = mystdout = StringIO()
                for job in qPending:
                    print 80*'-'
                    job.Dump()
                print 80*'-'
                sys.stdout = old_stdout
                queueLockP.release()
                returnString = str(mystdout.getvalue())
            elif action == "CHRUN":
                queueLockR.acquire()
                old_stdout = sys.stdout
                sys.stdout = mystdout = StringIO()
                for job in qRunning:
                    print 80*'-'
                    job.Dump()
                print 80*'-'
                sys.stdout = old_stdout
                queueLockR.release()
                returnString = str(mystdout.getvalue())
            elif action == "CHECK":
                returnString = """{} jobs to be submitted
{} jobs pending
{} jobs running
""".format(len(qToSubmit), nPending, nRunning)

        logging.debug(States.NAMES[STATE])
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
        testPort = subprocess.Popen([testPortCmd,''], shell=True, stdout=subprocess.PIPE).stdout.read().strip()
        logging.debug(testPort)
        if testPort == "":
            logging.info("Port " + str(hostport) + " is free")
            portLock.release()
            break
        else:
            logging.info("Port " + str(hostport) + " is used")
            hostport += 1
            pass
        sys.stderr.flush()

    run(host=hostname, port=hostport)
    logging.debug('exit')


def BackFillFromDB():
    global nPending
    global nRunning

    nFailed = 0
    prev_nFailed = 0

    failedJobs = []
    prev_failedJobs = []

    while True:
        logging.debug("BackFill")
        nPending = db.jobs.count({ #we want only jobs at our site
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : u'Pending'
        })

        nRunning = db.jobs.count({ #we want only jobs at our site
        u'user' : user,
        u'task' : task,
        u'from' : hostname,
        u'status' : u'Running'
        })

        for subtask in subTasks:
            subtask.Update()

        #----------------------------------------------------------------#
        # Query for failed jobs and re-queue them
        #----------------------------------------------------------------#
        nFailed = db.jobs.count({
        u'task' : task,
        u'status' : u'Error'
        })

        if nFailed != prev_nFailed:

            failedJobs = list( db.jobs.find({
            u'task' : task,
            u'status' : u'Error'
            }) )

            if len(failedJobs):
                logging.debug("Found " + str(nFailed) + " failed jobs in DB")

            for entry in [ x for x in failedJobs if x not in prev_failedJobs ]:
                job = FindJobFromHash( entry['hash'] )
                if not job:
                    continue
                if job.queued:
                    logging.debug("Job already queued")
                    continue
                # db.jobs.delete_one({'_id' : entry['_id']})
                if job.retries < 3:
                    job.queued = True
                    logging.debug("Queueing Job")
                    queueLockT.acquire()
                    qToSubmit.appendleft( job ) #if we submit it soon it a check less in this function later
                    queueLockT.release()
                else:
                    logging.info( "Failed processing for file {}".format(job.outfile) )

            prev_nFailed = nFailed
            prev_failedJobs = failedJobs
            pass

        PrintSummary()
        time.sleep( 3 )


def main():
    signal.signal( signal.SIGINT , SigHandler )

    parser = argparse.ArgumentParser(description='Job launcher for PG nuclei analysis.')
    parser.add_argument('-T' , '--task'       , type=str, nargs=1  , help='Task name. Choose wisely.')
    parser.add_argument('-e' , '--exe'        , type=str, nargs=1  , help='The executable to run')
    parser.add_argument('-ad', '--exeargsdata', type=str, nargs='*', help='Arguments to pass to the executable (data runs) without the leading - (value to be passed with =, e.g. A=B1082)')
    parser.add_argument('-am', '--exeargsmc'  , type=str, nargs='*', help='Arguments to pass to the executable (MC runs) without the leading - (value to be passed with =, e.g. A=B1082)')
    parser.add_argument('-c' , '--cwd'        , type=str, nargs=1  , help='The directory where the executable is located')
    parser.add_argument('-b' , '--batch'      , type=str, nargs=1  , help='Batch system (LSF / HTCondor)')
    parser.add_argument('-d' , '--dataset'    , type=str, nargs='+', help='Dataset list (can be "pass$N", or a mc build scheme such as "C.B1075.2_03")')
    parser.add_argument('-w' , '--overwrite'  , action='store_true', help='Overwrite existing output files')
    parser.add_argument('-i' , '--indir'      , type=str, nargs=1  , help='The main input directory (this is where input files are located. Site dependent)')
    parser.add_argument('-o' , '--outdir'     , type=str, nargs=1  , help='The main output directory (subdirs will be chosen by dataset type)')
    parser.add_argument(       '--hadd'       , action='store_true', help='Specify if hadd is required after execution')
    parser.add_argument('-q' , '--queue'      , type=str, nargs=1  , help='The queue to run on')
    parser.add_argument('-a' , '--amsvar'     , type=str, nargs=1  , help='Path to the amsvar script')
    parser.add_argument('-p' , '--port'       , type=str, nargs=1  , help='HTTP port to use (default=8080)')
    parser.add_argument('-H' , '--dbhost'     , type=str, nargs=1  , help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-U' , '--dbuser'     , type=str, nargs=1  , help='MongoDB user. Mandatory')
    parser.add_argument('-P' , '--dbpass'     , type=str, nargs=1  , help='MongoDB password. Mandatory')
    parser.add_argument('-m' , '--maxjobs'    , type=str, nargs=1  , help='Max number of jobs in queue')
    parser.add_argument('-bg', '--background' , action='store_true', help='Send process in the background (daemonize)')
    parser.add_argument('-t' , '--test'       , action='store_true', help='Test (don\'t submit any job...)')
    parser.add_argument('-g' , '--debug'      , action='store_true', help='Show debug info')

    global args
    args = parser.parse_args()

    SetupInputPar()

    isMC, filelist = GetFilelists( datasets, cwd )

    outlist = GetOutDir( outdir, datasets, filelist )

    global user
    user = subprocess.Popen(["whoami",''], shell=True, stdout=subprocess.PIPE).stdout.read().strip()

    sys.stdout.flush()

    global db, dbMgr

    dbMgr = MongoDBManager(dbhost, "dbarDB", dbuser, dbpass)
    dbMgr.Connect()

    db = dbMgr.db

    # All the stuff now happens here
    # acquire a lock to block port number
    global portLock
    portLock = threading.Lock()

    # acquire a lock to block queue manipulation
    # to allow indipendent queue edit setup a lock for each queue
    global queueLockT
    queueLockT = threading.Lock()

    portLock.acquire()
    # start HTTP listener on a separate thread
    threads = []
    lsn = threading.Thread(name='HTTP Listener', target=Listener)
    lsn.setDaemon(True)
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

    lsn.start()
    chk.start()

    #Prepare job queues
    sys.stdout.flush()

    for idata in range(0, len(datasets)):
        QueueJobs( filelist[idata], outlist[idata], argstring_mc if isMC[idata] else argstring_data, queue, datasets[idata] )

    bckfill.start()
    PrintSummary()

    #run machine
    FiniteStateMachine()


if __name__ == "__main__":

    main()
