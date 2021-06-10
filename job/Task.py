import os
import collections
import logging
import hashlib
import threading
import subprocess
import re

from dbmgr import MongoDBManager


class Task:
    def __init__(self, name):
        self.name = name
        self.exe = ""
        self.exeargs = ""
        self.workdir = ""
        self.jobscript = ""
        self.dataset = ""
        self.amsvar = ""
        self.indir = ""
        self.outdir = ""
        self.maxjobs = ""

        self.isNtuples = False
        self.subtasks = []

        self.totJobs = 0
        self.doneJobs = 0
        self.processingJobs = 0
        self.failedJobs = 0
        
        self.reducescript = None

        self.x509userproxy = ""  # works only on condor right now

    def Update(self):
        logging.debug("Task " + self.name + " updated:")
        updateThr = []
        for subtask in self.subtasks:
            updateThr.append(threading.Thread(
                target=subtask.Update, name="UpdaterThread-{}".format(subtask.name)))
            updateThr[-1].start()

        for subtask in self.subtasks:
            updateThr[self.subtasks.index(subtask)].join()
            subtask.Print()
            self.totJobs += subtask.totJobs
            self.doneJobs += subtask.doneJobs
            self.processingJobs += subtask.processingJobs
            self.failedJobs += subtask.failedJobs
