import pymongo
import urllib
from job.LSFJob import LSFJob
from job.HTCondorJob import HTCondorJob
import logging


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MongoDBManager:
    __metaclass__ = Singleton

    def __init__(self, host, db, user, pwd):
        self.host = host
        self.dbname = db
        self.db = None
        self.user = user
        self.pwd = pwd
        pass

    def Connect(self):
        # connect to db
        self.client = pymongo.MongoClient(
            'mongodb://{}:{}@{}/{}'.format(self.user, urllib.quote(self.pwd), self.host, self.dbname))
        self.db = self.client[self.dbname]

        if not 'jobs' in self.db.collection_names():
            self.db['jobs'].create_index(
                [('hash', pymongo.ASCENDING)], unique=True)
            self.db['jobs'].create_index(
                [('task', pymongo.ASCENDING)], unique=False)
            pass

        if not 'datasets' in self.db.collection_names():
            self.db['datasets'].create_index(
                [('dataset', pymongo.ASCENDING), ('subset', pymongo.ASCENDING)], unique=True)
            pass

        if not 'ntuples' in self.db.collection_names():
            self.db['ntuples'].create_index(
                [('dataset', pymongo.ASCENDING), ('subset', pymongo.ASCENDING)], unique=True)
            pass


    def InsertDataset(self, objdataset):

        print " - Inserting dataset {} subset {} into DB".format(objdataset['dataset'], objdataset['subset'])

        try:
            self.db.datasets.replace_one({
                u'dataset': objdataset['dataset'],
                u'subset': objdataset['subset']
            }, objdataset, upsert=True)
        except:
            print "   insertion failed."
            return False

        print "   successful."

        return True

    def InsertNtuples(self, objdataset):

        print " - Inserting ntuples dataset {} subset {} into DB".format(objdataset['dataset'], objdataset['subset'])

        try:
            self.db.ntuples.replace_one({
                u'dataset': objdataset['dataset'],
                u'subset': objdataset['subset']
            }, objdataset, upsert=True)
        except:
            print "   insertion failed."
            return False

        print "   successful."

        return True

    def InsertJob(self, job, status, ow=False, verifier=None):
        if isinstance(job, LSFJob):
            return self.InsertLSFJob(job, status, ow, verifier)
        elif isinstance(job, HTCondorJob):
            return self.InsertHTCondorJob(job, status, ow, verifier)
        else:
            logging.error("Unrecognized job type. DB insertion aborted.")
            return False

    def InsertLSFJob(self, job, status, ow=False, verifier=None):
        post = {
            u'type': u'LSFJob',
            u'hash': job.hash,
            u'jobName': job.jobName,
            u'lsfID': job.lsfID,
            u'outfile': job.outdir + '/' + job.outfile,
            u'retries': job.retries,
            u'from': job.site,
            u'user': job.user,
            u'task': job.task.name,
            u'subtask': job.subtask,
            u'status': status
        }

        if ow:
            self.db.jobs.replace_one({
                u'hash': job.hash,
                u'task': job.task.name
            }, post, upsert=True)
            return True
        else:
            try:
                self.db.jobs.insert_one(post)
            except pymongo.errors.DuplicateKeyError:
                logging.warning(
                    "Job {} already in DB when inserting. Skipping.".format(job.hash))
                return False
            return True

        pass

    def InsertHTCondorJob(self, job, status, ow=False, verifier=None):
        post = {
            u'type': u'HTCondorJob',
            u'hash': job.hash,
            u'jobName': job.jobName,
            u'condorID': job.condorID,
            u'outfile': job.outdir + '/' + job.outfile,
            u'retries': job.retries,
            u'from': job.site,
            u'user': job.user,
            u'task': job.task.name,
            u'subtask': job.subtask,
            u'status': status
        }

        if ow:
            self.db.jobs.replace_one({
                u'hash': job.hash,
                u'task': job.task.name
            }, post, upsert=True)
            return True
        else:
            try:
                self.db.jobs.insert_one(post)
            except pymongo.errors.DuplicateKeyError:
                logging.warning(
                    "Job {} already in DB when inserting. Skipping.".format(job.hash))
                return False
            return True

        pass

    def UpdateJobStatus(self, job, status):
        self.db.jobs.update_one({
            u'hash': job.hash,
            u'task': job.task.name
        }, {'$set': {u"status": status}
            })
        return True

    def UpdateJobWithID(self, job, subStatus):
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
                u'hash': job.hash,
                u'task': job.task.name
            }, {'$set': {field: value}
                })
            return True
        else:
            logging.error('Not submitted')
            self.db.jobs.update_one({
                u'hash': job.hash,
                u'task': job.task.name
            }, {'$set': {u'status': u'Error'}
                })
            return False

    def CleanTaskFromDB(self, taskname):
        self.db.jobs.delete_many({
            u'task': taskname
        })

        return True
