import os
import collections
import logging
import hashlib
import threading
import subprocess, re

import dbmgr

#----------------------------------------------------------------#
# Class to manage a LSF Job
#----------------------------------------------------------------#
class HTCondorJob:

# arguments               @=arg
# {}
# @arg

    CondorTemplate = """executable              = {}
input                   = {}
output                  = {}
error                   = {}
log                     = {}
batch_name              = {}

should_transfer_files	= YES
transfer_input_files    = {}

universe                = vanilla
+MaxRuntime             = 14000
#+JobFlavour            = "workday"
RequestCpus             = 1

queue 1"""

    def __init__( self, i, subtask ):
        self.useCWD = True
        self.i = i
        self.totfiles = subtask.totJobs
        self.jobName = ""
        self.hash = ""
        self.amsvar = ""
        self.exe = ""
        self.workdir = ""
        self.arguments = ""
        self.indir = ""
        self.outdir = ""
        self.infile = ""
        self.outfile = ""
        self.task = subtask.task
        self.subtask = subtask.name
        self.queue = ""     #not needed?
        self.pinfile = ""
        self.poutfile = ""
        self.perrfile = ""
        self.flags = ""
        self.script = ""
        self.condorID = 0      #internal variable, will be used to check job status
        self.retries = 0
        self.notFoundPending = False
        self.shortID = ""
        self.user = ""
        self.site = ""
        self.server = dbmgr.MongoDBManager().host
        self.db     = dbmgr.MongoDBManager().dbname
        self.dbuser = dbmgr.MongoDBManager().user
        self.passwd = dbmgr.MongoDBManager().pwd
        self.condorfile = ""
        self.queued = False
        self.stdinstring = ""

    def SetName( self ):
        self.jobName += str(self.i) + "/" + str(self.totfiles) + "-" + os.path.basename(self.exe) + "-"
        self.jobName += self.task + "-" + self.flags + "-"
        self.jobName += os.path.basename(self.infile.strip())
        self.hash     = hashlib.sha1(self.jobName).hexdigest()

    def FillVars( self ):
        self.pinfile  = self.workdir+"/pin/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.perrfile = self.workdir+"/perr/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.poutfile = self.workdir+"/pout/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.plogfile = self.workdir+"/pout/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".log"

    def GetCMD( self ):
        self.stdinstring  = self.amsvar
        self.stdinstring += '\n'

        self.stdinstring += self.exe + " " + self.arguments
        # if( len(masses) ):
        #     self.stdinstring += " -a" + masses[ilist]
        self.stdinstring += '\n'

        self.stdinstring += self.indir
        self.stdinstring += '\n'

        self.stdinstring += self.outdir
        self.stdinstring += '\n'

        self.stdinstring += self.infile
        self.stdinstring += '\n'

        self.stdinstring += self.outfile
        self.stdinstring += '\n'

        self.stdinstring += self.dbuser
        self.stdinstring += '\n'

        self.stdinstring += self.passwd
        self.stdinstring += '\n'

        self.stdinstring += self.server + "/" + self.db
        self.stdinstring += '\n'

        self.stdinstring += self.hash

        self.cmd = "condor_submit {}/.tmp/{}.sub".format(self.workdir, self.hash)

        # self.cmd  = "bsub -q " + self.queue
        # if self.useCWD:
        #     self.cmd += " -cwd " + self.workdir
        # self.cmd += " -eo " + self.perrfile
        # self.cmd += " -oo " + self.poutfile
        # self.cmd += " -J \"" + self.jobName + "\""
        # self.cmd += " \"" + self.script + " <<< " + "\\\"" + stdinstring + "\\\"\""

        # return self.condorfile

    def SwitchToQueue( self, queue ):
        switchCmd = "bswitch " + str(queue) + " " + str(self.lsfID)
        dummy = subprocess.Popen([switchCmd,''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        dummyOut = dummy.stdout.read().strip()
        dummyErr = dummy.stderr.read().strip()
        if dummyErr != "":
            logging.error( dummyErr )
            pass

    def Submit( self ):
        self.GetCMD()

        with open(self.pinfile, "w") as pinfile:
            logging.debug("Writing stdin file...")
            pinfile.write(self.stdinstring)
            logging.debug("... done")


        self.condorfile = self.CondorTemplate.format(
            self.script,
            # "\' <<< " + "\\\"" + stdinstring + "\\\"\'",
            self.pinfile,
            self.poutfile,
            self.perrfile,
            self.plogfile,
            self.task,
            self.amsvar
        )

        if not os.path.exists( self.workdir + ".tmp" ):
            logging.info(".tmp directory missing. Creating...")
            os.makedirs( self.workdir + ".tmp" )
            os.chmod( self.workdir + ".tmp", 0775 )
            logging.info("... done")
            pass

        with open(self.workdir + ".tmp/{}.sub".format(self.hash), "w") as submitFile:
            logging.debug("Writing condor file...")
            submitFile.write(self.condorfile)
            logging.debug("... done")

        proc = subprocess.Popen([self.cmd,''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        kill_proc = lambda p: p.kill()
        timer = threading.Timer(60, kill_proc, [proc])
        try:
            timer.start()
            dummyOut, dummyErr = proc.communicate()
            logging.debug("condor_submit stdout: " + dummyOut.strip())
            logging.debug("condor_submit stderr: " + dummyErr.strip())
        finally:
            timer.cancel()

        if dummyErr != "":
            logging.error( dummyErr )
            pass

        try:
            self.condorID = int(re.search( ".*cluster\s(\d*)", dummyOut ).group(1))
        except AttributeError:
            logging.error( "condor_submit command probably failed. Please check for problems..." )
            return False
        self.shortID = str(self.condorID)
        os.remove(self.workdir + ".tmp/{}.sub".format(self.hash))
        logging.debug("condor file removed")

        return True

    def Dump( self ):
        print "useCWD     = ", self.useCWD
        print "i          = ", self.i
        print "totfiles   = ", self.totfiles
        print "jobName    = ", self.jobName
        print "hash       = ", self.hash
        print "amsvar     = ", self.amsvar
        print "exe        = ", self.exe
        print "workdir    = ", self.workdir
        print "arguments  = ", self.arguments
        print "outdir     = ", self.outdir
        print "infile     = ", self.infile
        print "outfile    = ", self.outfile
        print "queue      = ", self.queue
        print "poutfile   = ", self.poutfile
        print "perrfile   = ", self.perrfile
        print "flags      = ", self.flags
        print "script     = ", self.script
        print "retries    = ", self.retries
        print "server     = ", self.server
        print "condorfile = ", self.condorfile
