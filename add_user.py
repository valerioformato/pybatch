import random, string
import sys
import argparse
import subprocess
import urllib
import pymongo

def main():
    parser = argparse.ArgumentParser(description='Create a new user in the admin DB.')
    parser.add_argument('-H' , '--dbhost'     , type=str, nargs=1  , help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-U' , '--dbuser'     , type=str, nargs=1  , help='MongoDB user. Mandatory')
    parser.add_argument('-P' , '--dbpass'     , type=str, nargs=1  , help='MongoDB password. Mandatory')
    parser.add_argument('-D' , '--db'         , type=str, nargs=1  , help='MongoDB db name. Mandatory')
    args = parser.parse_args()

    if not args.dbuser:
        print "Error: you need to provide an admin user to log on the DB (-U option)"
        sys.exit(1)
        pass

    if not args.dbpass:
        print "Error: you need to provide an admin password to log on the DB (-P option)"
        sys.exit(1)
        pass

    if not args.db:
        print "Error: you need to provide the name of the DB (-D option)"
        sys.exit(1)
        pass

    dbuser = args.dbuser[0]
    dbpass = args.dbpass[0]
    dbname = args.db[0]
    hostname = str(subprocess.Popen(['hostname',''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip())

    global dbhost
    if args.dbhost:
        dbhost = str( args.dbhost[0] )
    else:
        dbhost = hostname

    client = pymongo.MongoClient('mongodb://{}:{}@{}/{}'.format(dbuser, urllib.quote(dbpass), dbhost, 'admin'))
    db = client[dbname]
    print db

    username = str( raw_input("Enter user name: ") )
    password = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

    print "Creating user {} on DB {}".format(username, dbname)
    print "Temporary password: {}\n== Don't lose it until you change it! ==".format(password)
    try:
        db.add_user(
        username, password,
        roles=[
        { 'role' : 'readWrite', 'db' : dbname },
        { 'role' : 'changeOwnPasswordCustomDataRole', 'db' : 'admin' }
        ])
    except Exception as e:
        print "Error, check what you are doing..."
        print type(e), e
        return

    print "Done!"

if __name__ == "__main__":
    main()
