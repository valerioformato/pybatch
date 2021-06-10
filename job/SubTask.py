import os
import collections
import logging
import hashlib
import threading
import subprocess
import re

from LSFJob import LSFJob
from HTCondorJob import HTCondorJob

from dbmgr import MongoDBManager


class SubTask:
    def __init__(self, name, task):
        self.name = name
        self.task = task
        self.totJobs = 0
        self.doneJobs = 0
        self.processingJobs = 0
        self.failedJobs = 0
        self.subsets = []
        self.reduceJobs = []
        self.jobList = collections.deque()

        self.indir = []
        self.outdir = []
        self.isMC = False
        self.runlist = []
        self.isReduced = False

        self.lock = threading.Lock()

    def FindJob(self, hash):
        self.lock.acquire()
        jobSearch = [x for x in self.jobList if x.hash == hash]
        self.lock.release()
        if len(jobSearch) > 0:
            return jobSearch[0]
        else:
            return None
        pass

    def RemoveJob(self, hash):
        self.lock.acquire()
        jobSearch = [x for x in self.jobList if x.hash == hash]
        if len(jobSearch) > 0:
            self.jobList.remove(jobSearch[0])
            pass
        self.lock.release()

    def AddJob(self, job):
        self.lock.acquire()
        self.jobList.append(job)
        self.lock.release()

    def Update(self):
        db = MongoDBManager().db

        query = db.jobs.find({u'task': self.task.name,
                              u'subtask': self.name}, {u'status': 1, u'_id': 0})

        doneJobs, processingJobs, failedJobs = 0, 0, 0

        for job in query:
            if job[u'status'] == 'Done':
                doneJobs += 1
            if job[u'status'] == 'Pending' or job[u'status'] == 'Running':
                processingJobs += 1
            if job[u'status'] == 'Error':
                failedJobs += 1

        #logging.debug("{} {} {}".format(doneJobs, processingJobs, failedJobs) )
        self.doneJobs, self.processingJobs, self.failedJobs = doneJobs, processingJobs, failedJobs

    def Print(self):
        if self.totJobs > 0:
            logging.debug("  SubTask " + self.name + " updated:")
            logging.debug("   - {}/{} done. ({}%)".format(self.doneJobs,
                                                          self.totJobs, 100*float(self.doneJobs)/self.totJobs))
            logging.debug("   - {}/{} processing. ({}%)".format(self.processingJobs,
                                                                self.totJobs, 100*float(self.processingJobs)/self.totJobs))
            logging.debug("   - {}/{} failed. ({}%)".format(self.failedJobs,
                                                            self.totJobs, 100*float(self.failedJobs)/self.totJobs))
            logging.debug("   ------- Reduce job " +
                          ("already" if self.isReduced else "not") + " submitted")

    def IsDone(self):
        if (self.processingJobs == 0) and (self.doneJobs + self.failedJobs == self.totJobs):
            return True
        else:
            return False

    def PrepareReduceJobs(self, batch, queue, user, hostname, condorName=None, condorPool=None):
        icount = self.totJobs + 1

        if batch == "LSF":
            job = LSFJob(icount, self)
        elif batch == "HTCondor":
            job = HTCondorJob(icount, self)

        job.useCWD = False
        job.workdir = self.task.workdir
        job.amsvar = self.task.amsvar
        job.exe = "hadd"
        job.script = self.task.reducescript
        job.indir = ""
        job.outdir = self.task.outdir
        for _dir in self.outdir:
            job.infile += "{}/*.root ".format(_dir)
        job.outfile = "{}.root".format(self.name) if self.isMC else "{}.root".format(self.name.split('/')[1])
        job.queue = queue
        job.user = user
        job.site = hostname

        job.FillVars()
        job.SetName()
        job.GetCMD()
        if batch=="HTCondor" and condorName:
            job.SetSchedd(condorName, condorPool)

        logging.debug("Reduce step prepared:")
        job.Dump()

        self.reduceJobs.append(job)
