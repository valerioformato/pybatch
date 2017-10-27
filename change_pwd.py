import sys
import argparse
import subprocess
import urllib
from getpass import getpass
import pymongo

def main():
    parser = argparse.ArgumentParser(description='Create a new user in the admin DB.')
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
    db = client[dbname]

    nope = True
    while nope:
        password1 = getpass("Type in your new password: ")
        password2 = getpass("Confirm your new password: ")

        if password1 != password2:
            nope = True
            print "Passwords don't match, try again...\n\n"
        else:
            nope = False

    try:
        db.command({
        'updateUser' : dbuser,
        'pwd' : password1,
        })
    except pymongo.errors.OperationFailure as e:
        print "ERROR:", e
        sys.exit(1)

    print "Done!"

if __name__ == "__main__":
    main()
