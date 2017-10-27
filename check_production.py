import sys
import argparse
import subprocess
import urllib

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

    histos = []
    for task in tasks:
        histos.append( ROOT.TH1D("hist_{}".format(task), ";t;Jobs", 500, 0, 12))
        query = list(db.jobs.find({
        u'task' : task,
        u'status' : u'Done'
        }))
        print "Queried {} jobs for task {}".format(len(query), task)
        for job in query:
            try:
                histos[-1].Fill((job['finishTime']-job['startTime']).total_seconds()/float(3600))
            except KeyError:
                continue

    return histos



def main():
    parser = argparse.ArgumentParser(description='Some DB plots...')
    parser.add_argument('-H' , '--dbhost'     , type=str, nargs=1  , help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-U' , '--dbuser'     , type=str, nargs=1  , help='MongoDB user. Mandatory')
    parser.add_argument('-P' , '--dbpass'     , type=str, nargs=1  , help='MongoDB password. Mandatory')
    parser.add_argument('-D' , '--db'         , type=str, nargs=1  , help='MongoDB db name. Mandatory')
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

    timehistos = PlotDuration()
    print timehistos

    outfile = ROOT.TFile("prod_plots.root", "recreate")
    for histo in timehistos:
        outfile.WriteTObject(histo)
    outfile.Close()

if __name__ == "__main__":
    main()
