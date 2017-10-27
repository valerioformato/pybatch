import pymongo
import urllib
from   job.LSFJob import LSFJob
from   job.HTCondorJob import HTCondorJob
import logging

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MongoDBManager:
    __metaclass__ = Singleton
    def __init__( self, host, db, user, pwd ):
        self.host   = host
        self.dbname = db
        self.db     = None
        self.user   = user
        self.pwd    = pwd
        pass

    def Connect( self ):
        #connect to db
        self.client = pymongo.MongoClient('mongodb://{}:{}@{}/{}'.format(self.user, urllib.quote(self.pwd), self.host, self.dbname))
        self.db = self.client[self.dbname]

        print self.client, self.db

        if not 'jobs' in  self.db.collection_names():
            self.db['jobs'].create_index([('task', pymongo.ASCENDING), ('hash', pymongo.ASCENDING)], unique=True)
            pass

    def InsertTask( self, task ):
        #tasks to be implemented
        pass

    def InsertJob( self, job, status, ow=False ):
        if isinstance(job, LSFJob):
            return self.InsertLSFJob( job, status, ow )
        elif isinstance(job, HTCondorJob):
            return self.InsertHTCondorJob( job, status, ow )
        else:
            logging.error("Unrecognized job type. DB insertion aborted.")
            return False


    def InsertLSFJob( self, job, status, ow=False ):
        post = {
        u'type'    : u'LSFJob',
        u'hash'    : job.hash,
        u'jobName' : job.jobName,
        u'lsfID'   : job.lsfID,
        u'outfile' : job.outdir + '/' + job.outfile,
        u'retries' : job.retries,
        u'from'    : job.site,
        u'user'    : job.user,
        u'task'    : job.task,
        u'subtask' : job.subtask,
        u'status'  : status
        }

        if ow:
            self.db.jobs.replace_one({
            u'hash' : job.hash,
            u'task' : job.task
            }, post, upsert=True )
            return True
        else:
            try:
                self.db.jobs.insert_one( post )
            except pymongo.errors.DuplicateKeyError:
                logging.warning("Job {} already in DB when inserting. Skipping.")
                return False
            return True

        pass

    def InsertHTCondorJob( self, job, status, ow=False ):
        post = {
        u'type'     : u'HTCondorJob',
        u'hash'     : job.hash,
        u'jobName'  : job.jobName,
        u'condorID' : job.condorID,
        u'outfile'  : job.outdir + '/' + job.outfile,
        u'retries'  : job.retries,
        u'from'     : job.site,
        u'user'     : job.user,
        u'task'     : job.task,
        u'subtask'  : job.subtask,
        u'status'   : status
        }

        if ow:
            self.db.jobs.replace_one({
            u'hash' : job.hash,
            u'task' : job.task
            }, post, upsert=True )
            return True
        else:
            try:
                self.db.jobs.insert_one( post )
            except pymongo.errors.DuplicateKeyError:
                logging.warning("Job {} already in DB when inserting. Skipping.")
                return False
            return True

        pass


    def UpdateJobWithID( self, job, subStatus ):
        field = ""
        value = ""

        if isinstance(job, LSFJob):
            field = u'lsfID'
            value = str(job.lsfID)
        elif isinstance(job, HTCondorJob):
            field = u'condorID'
            value = str(job.condorID)

        if subStatus:
            self.db.jobs.update_one({
            u'hash' : job.hash,
            u'task' : job.task
            }, { '$set' : { field : value }
            })
            return True
        else:
            logging.error('Not submitted')
            self.db.jobs.update_one({
            u'hash' : job.hash,
            u'task' : job.task
            }, { '$set' : { u'status' : u'Error'}
            })
            return False


    def CleanTaskFromDB( self, taskname ):
        nRunning = self.db.jobs.count({
        u'task' : taskname,
        u'status' : u'Running'
        })

        if nRunning > 0:
            logging.error("DB cleanup attempted, but running jobs found in the DB for the required task ({})".format(taskname))
            return False

        nPending = self.db.jobs.count({
        u'task' : taskname,
        u'status' : u'Pending'
        })

        if nPending > 0:
            logging.error("DB cleanup attempted, but pending jobs found in the DB for the required task ({})".format(taskname))
            return False

        self.db.jobs.delete_many({
        u'task' : taskname
        })

        return True
