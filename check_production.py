import sys
import argparse
import subprocess
import urllib
import pprint

import ROOT
import pymongo

def PlotDuration():
    global db

    pipeline = [
    { u'$group' : {u'_id' : u'$task'} }
    ]
    print pipeline

    query = list(db.jobs.aggregate(pipeline))
    print query

    tasks = [x['_id'] for x in query]
    print tasks

    global histos
    histos = {}
    for task in tasks:
        print 80*'='
        PlotTask(task)
        print 80*'='
    return histos

def PlotTask(task):
    pipeline = [
    { u'$match' : {u'task' : task} },
    { u'$group' : {u'_id' : u'$subtask'} }
    ]
    print pipeline

    query = list(db.jobs.aggregate(pipeline))
    print 80*'-'

    temphistos = {}

    subtasks = [x['_id'] for x in query]
    for subtask in subtasks:
        temphistos[subtask] = ROOT.TH1D("hist_{}_{}".format(task.replace('/', '_'), subtask.replace('/', '_')), ";t;Jobs", 400, 0, 36)
        query = list(db.jobs.find({
        u'task' : task,
        u'subtask' : subtask,
        u'status' : u'Done'
        }))
        print "Queried {} jobs for task {} - {}".format(len(query), task, subtask)
        for job in query:
            try:
                temphistos[subtask].Fill((job['finishTime']-job['startTime']).total_seconds()/float(3600))
            except KeyError:
                continue

    histos[task] = temphistos



def main():
    parser = argparse.ArgumentParser(description='Some DB plots...')
    parser.add_argument('-H' , '--dbhost'     , type=str, nargs=1  , help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-U' , '--dbuser'     , type=str, nargs=1  , help='MongoDB user. Mandatory')
    parser.add_argument('-P' , '--dbpass'     , type=str, nargs=1  , help='MongoDB password. Mandatory')
    parser.add_argument('-D' , '--db'         , type=str, nargs=1  , help='MongoDB db name. Mandatory')
    parser.add_argument('-t' , '--task'       , type=str, nargs=1  , help='Task name.')
    args = parser.parse_args()

    if not args.dbuser:
        print "Error: you need to provide a user to log on the DB (-U option)"
        sys.exit(1)
        pass

    if not args.db:
        print "Error: you need to provide the name of the DB (-D option)"
        sys.exit(1)
        pass

    dbuser = args.dbuser[0]
    if args.dbpass:
        dbpass = args.dbpass[0]
    dbname = args.db[0]
    hostname = str(subprocess.Popen(['hostname',''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip())

    global dbhost
    if args.dbhost:
        dbhost = str( args.dbhost[0] )
    else:
        dbhost = hostname

    if not args.dbpass:
        dbpass = getpass("Type in your old password: ")

    client = pymongo.MongoClient('mongodb://{}:{}@{}/{}'.format(dbuser, urllib.quote(dbpass), dbhost, dbname))
    global db
    db = client[dbname]

    print db

    global task
    if args.task:
        task = args.task

    timehistos = PlotDuration()
    pprint.pprint( timehistos )

    outfile = ROOT.TFile("prod_plots.root", "recreate")
    for task, histos in timehistos.iteritems():
        outfile.cd()
        task = task.replace('/','_')
        tdir = ROOT.TDirectoryFile(task, task)
        tdir.cd()
        for subtask, histo in histos.iteritems():
            subtask = subtask.replace('/','_')
            # print task, subtask, histo.GetSumOfWeights()
            histo.SetName(subtask)
            histo.Write()
        tdir.ls()
        tdir.Write()
    outfile.Close()

if __name__ == "__main__":
    main()
