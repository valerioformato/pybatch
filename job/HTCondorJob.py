import os
import collections
import logging
import hashlib
import threading
import subprocess, re

import dbmgr

CondorTemplate = """workdir = {}
ioname = {}

executable              = {}
input                   = $(workdir)/pin/$(ioname).txt
output                  = /dev/null
error                   = /dev/null
log                     = /dev/null
batch_name              = {}

should_transfer_files	= YES
transfer_input_files    = {}

stream_error            = False
stream_output           = False

universe                = vanilla
+MaxRuntime             = 86400
RequestCpus             = 1
request_disk            = 2GB

periodic_hold           = ((JobStatus==2) && (CurrentTime - EnteredCurrentStatus) > 2700 && (RemoteSysCpu + RemoteUserCpu) > 600 && (RemoteSysCpu + RemoteUserCpu)/(CurrentTime - EnteredCurrentStatus) < 0.02)
periodic_release        = (CurrentTime - EnteredCurrentStatus) > 300

environment             = "STDINFILENAME=$(workdir)/pin/$(ioname).txt"

requirements            = (OpSysAndVer =?= "CentOS7")
+AMSPublic              = true

{}

queue 1"""

#----------------------------------------------------------------#
# Class to manage a LSF Job
#----------------------------------------------------------------#
class HTCondorJob:

    def __init__( self, i, subtask ):
        self.useCWD = True
        self.i = i
        self.totfiles = subtask.totJobs
        self.jobName = ""
        self.hash = ""
        self.amsvar = ""
        self.keytab = None
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
        self.ioname = ""
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
        self.test = False
        self.userschedd = False
        self.scheddname = None
        self.scheddpool = None


    def SetSchedd( self, scheddname, scheddpool = None):
        self.userschedd = True
        self.scheddname = scheddname
        self.scheddpool = scheddpool
        pass

    def SetName( self ):
        # self.jobName += str(self.i) + "/" + str(self.totfiles) + "-" + os.path.basename(self.exe) + "-"
        self.jobName  = os.path.basename(self.exe) + "-"
        self.jobName += self.task.name + "-" + self.subtask + "-"
        self.jobName += os.path.basename(self.outfile.strip())

        self.hash     = os.path.basename(self.exe) + "-"
        self.hash    += self.arguments + "-"
        self.hash    += self.task.name + "-" + self.subtask + "-"
        self.hash    += os.path.basename(self.outfile.strip())
        self.hash     = hashlib.sha1(self.hash).hexdigest()

    def FillVars( self ):
        self.ioname   = os.path.basename(self.exe)+"_"+self.outfile[:-5]
        self.pinfile  = self.workdir+"/pin/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.perrfile = self.workdir+"/perr/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.poutfile = self.workdir+"/pout/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.plogfile = self.workdir+"/pout/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".log"

    def GetCMD( self ):
        self.stdinstring  = self.amsvar
        self.stdinstring += '\n'

        self.stdinstring += self.exe + " " + self.arguments
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

        self.cmd =  "condor_submit "
        if self.userschedd:
            self.cmd += "-name {} ".format(self.scheddname)
        if self.scheddpool:
            self.cmd += "-pool {} ".format(self.scheddpool)
        self.cmd += "-spool "
        self.cmd += "{}/.tmp/{}.sub".format(self.workdir, self.hash)

    def Submit( self ):
        self.GetCMD()
        if self.test:
            print self.cmd

        with open(self.pinfile, "w") as pinfile:
            logging.debug("Writing stdin file...")
            pinfile.write(self.stdinstring)
            logging.debug("... done")


        toBeTransf = "/bin/echo"
        toBeTransf += ", {}".format(self.amsvar)
        if self.keytab:
            toBeTransf += ", {}".format(self.keytab)
        self.condorfile = CondorTemplate.format(
            self.workdir,
            self.ioname,
            self.script,
            self.task.name,
            toBeTransf,
            "x509userproxy = {}".format(self.task.x509userproxy) if (self.task.x509userproxy != "") else ""
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

        if self.test:
            return

        proc = subprocess.Popen([self.cmd,''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        dummyOut, dummyErr = proc.communicate()
        logging.debug("condor_submit stdout: " + dummyOut.strip())
        logging.debug("condor_submit stderr: " + dummyErr.strip())

        if dummyErr != "":
            logging.error( dummyErr.strip() )
            pass

        try:
            self.condorID = int(re.search( ".*cluster\s(\d*)", dummyOut ).group(1))
        except AttributeError:
            logging.error( "condor_submit command probably failed. Please check for problems..." )
            logging.error("condor_submit stdout: " + dummyOut.strip())
            logging.error("condor_submit stderr: " + dummyErr.strip())
            return False
        logging.debug("Retrieved condorID: {}".format(self.condorID))
        self.shortID = str(self.condorID)
        try:
            os.remove(self.workdir + ".tmp/{}.sub".format(self.hash))
        except OSError:
            pass
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
        print "script     = ", self.script
        print "retries    = ", self.retries
        print "server     = ", self.server
        print "condorfile = ", self.condorfile
