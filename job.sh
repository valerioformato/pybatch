#!/bin/bash

read amsvar
read exec
read indir
read outdir
read filelist
read outfile
read dbuser
read passwd
read serverDB
read jobhash

source $amsvar
user=`whoami`

if [ `echo \`hostname\` | grep cnaf` ]; then
    echo $KRB5_CONFIG >&2
    ls -lah /storage/gpfs_ams/ams/users/$user/.kerberos/vformato.keytab
    echo    kinit vformato@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/vformato.keytab
    kinit vformato@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/vformato.keytab
    WORKDIR=`mktemp -d -p /data job.$user.XXXXXXXXXX`
else
    echo $filelist >&2
    ls -lah $filelist >&2
    WORKDIR=`mktemp -d -p ./ job.$user.XXXXXXXXXX`
fi



ls -lah `echo $exec | awk '{print $1}'`

queryTries=0
queryStatus=1
while [ $queryStatus -gt 0 ]; do
    echo mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Running\"}, \$currentDate : {\"startTime\" : true}});" $serverDB
    mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Running\"}, \$currentDate : {\"startTime\" : true}});" $serverDB
    queryStatus=$?
    echo "exit status: " $queryStatus
    sleep 1
    let "queryTries=$queryTries + 1"
    if [ $queryTries -gt 60 ]; then
        exit 1
    fi
done

$exec -d $indir -r $filelist -o $WORKDIR/$outfile
rv=$?

localfile=$WORKDIR/$outfile
remotefile=$outdir/`basename $outfile`
echo cp $localfile $remotefile
cp $localfile $remotefile
hash1=`md5sum $localfile | awk '{print $1}'`
hash2=`md5sum $remotefile | awk '{print $1}'`
chmod g+w $remotefile

if [ ! -e $remotefile -o $hash1 != $hash2 ]; then
  rv=1
fi

echo rm -rf $WORKDIR
rm -rf $WORKDIR


queryTries=0
queryStatus=1
if [[ $rv != 0 ]]; then
    while [ $queryStatus -gt 0 ]; do
        echo mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Error\"}});" $serverDB
        mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Error\"}});" $serverDB
        queryStatus=$?
        echo "exit status: " $queryStatus
        sleep 1
        let "queryTries=$queryTries + 1"
        if [ $queryTries -gt 60 ]; then
            exit 1
        fi
    done
else
    while [ $queryStatus -gt 0 ]; do
        echo mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Done\"}, \$currentDate : {\"finishTime\" : true}});" $serverDB
        mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Done\"}, \$currentDate : {\"finishTime\" : true}});" $serverDB
        queryStatus=$?
        echo "exit status: " $queryStatus
        sleep 1
        let "queryTries=$queryTries + 1"
        if [ $queryTries -gt 60 ]; then
            exit 1
        fi
    done
fi

exit
