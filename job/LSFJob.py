import os
import collections
import logging
import hashlib
import threading
import subprocess, re

import dbmgr


def kill_proc(proc):
    logging.warning("Timeout reached, attempting to kill process...")
    proc.kill()

#----------------------------------------------------------------#
# Class to manage a LSF Job
#----------------------------------------------------------------#
class LSFJob:
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
        self.queue = ""
        self.poutfile = ""
        self.perrfile = ""
        self.script = ""
        self.lsfID = 0      #internal variable, will be used to check job status
        self.retries = 0
        self.notFoundPending = False
        self.shortID = ""
        self.user = ""
        self.site = ""
        self.server = dbmgr.MongoDBManager().host
        self.db     = dbmgr.MongoDBManager().dbname
        self.dbuser = dbmgr.MongoDBManager().user
        self.passwd = dbmgr.MongoDBManager().pwd
        self.cmd = ""
        self.queued = False
        self.test = False

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
        # self.perrfile = self.workdir+"/perr/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        # self.poutfile = self.workdir+"/pout/"+os.path.basename(self.exe)+"_"+self.outfile[:-5]+".txt"
        self.perrfile = "/dev/null"
        self.poutfile = "/dev/null"

    def GetCMD( self ):
        stdinstring  = self.amsvar
        stdinstring += '\n'

        stdinstring += self.exe + " " + self.arguments
        # if( len(masses) ):
        #     stdinstring += " -a" + masses[ilist]
        stdinstring += '\n'

        stdinstring += self.indir
        stdinstring += '\n'

        stdinstring += self.outdir
        stdinstring += '\n'

        stdinstring += self.infile
        stdinstring += '\n'

        stdinstring += self.outfile
        stdinstring += '\n'

        stdinstring += self.dbuser
        stdinstring += '\n'

        stdinstring += self.passwd
        stdinstring += '\n'

        stdinstring += self.server + "/" + self.db
        stdinstring += '\n'

        stdinstring += self.hash

        self.cmd  = "bsub -q " + self.queue
        if self.useCWD:
            self.cmd += " -cwd " + self.workdir
        self.cmd += " -eo " + self.perrfile
        self.cmd += " -oo " + self.poutfile
        self.cmd += " -J \"" + self.jobName + "\""
        self.cmd += " \"" + self.script + " <<< " + "\\\"" + stdinstring + "\\\"\""

        return self.cmd

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
        if self.test:
            print self.cmd
            return
        
        proc = subprocess.Popen([self.cmd,''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # kill_proc = lambda p: p.kill()
        timer = threading.Timer(60, kill_proc, [proc])
        try:
            timer.start()
            dummyOut, dummyErr = proc.communicate()
            logging.debug("bsub stdout: " + dummyOut.strip())
            logging.debug("bsub stderr: " + dummyErr.strip())
        finally:
            timer.cancel()

        if dummyErr != "":
            logging.error( dummyErr )
            pass

        try:
            self.lsfID = int(re.search( "<([0-9]+)>", dummyOut ).group(1))
        except AttributeError:
            logging.error( "bsub command probably failed. Please check for problems..." )
            return False
        self.shortID = str(self.lsfID)
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
        print "script  = ", self.script
        print "retries    = ", self.retries
        print "server     = ", self.server
        print "cmd        = ", self.cmd
