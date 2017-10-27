import os
import collections
import logging
import hashlib
import threading
import subprocess, re

from dbmgr import MongoDBManager

class SubTask:
    def __init__( self, name, taskname ):
        self.name = name
        self.task = taskname
        self.totJobs = 0
        self.doneJobs = 0
        self.processingJobs = 0
        self.failedJobs = 0
        self.postCmd = ""
        self.jobList = collections.deque()

    def Update( self ):
        db = MongoDBManager().db

        self.doneJobs = db.jobs.count({
        u'task' : self.task,
        u'subtask' : self.name,
        u'status' : u'Done'
        })

        self.processingJobs = db.jobs.count({
        u'$or' :
          [{
            u'task' : self.task,
            u'subtask' : self.name,
            u'status' : u'Pending'
          },
          {
            u'task' : self.task,
            u'subtask' : self.name,
            u'status' : u'Running'
          }]
        })

        self.failedJobs = db.jobs.count({
        u'task' : self.task,
        u'subtask' : self.name,
        u'status' : u'Error'
        })

        logging.debug("SubTask " + self.name + " updated:")
        logging.debug(" - {}/{} done. ({}%)".format(self.doneJobs, self.totJobs, 100*float(self.doneJobs)/self.totJobs))
        logging.debug(" - {}/{} processing. ({}%)".format(self.processingJobs, self.totJobs, 100*float(self.processingJobs)/self.totJobs))
        logging.debug(" - {}/{} failed. ({}%)".format(self.failedJobs, self.totJobs, 100*float(self.failedJobs)/self.totJobs))

    def IsDone( self ):
        if (self.processingJobs == 0) and (self.doneJobs + self.failedJobs == self.totJobs):
            return True
        else:
            return False
