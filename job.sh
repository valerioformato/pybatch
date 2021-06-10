#!/bin/bash
# <<<< Job template for pybatch >>>>
# This is supposed to work with very minimal modifications
# The input variables should suffice for every use case
#   if you find yourself needing one more, chances are
#   you missed some of the options in the pybatch launcher

read amsvar
read userexe
read indir
read outdir
read filelist
read outfile
read dbuser
read passwd
read serverDB
read jobhash

user=`whoami`

if [ `echo \`hostname\` | grep cnaf` ]; then
  echo $KRB5_CONFIG >&2
  if [ -e /storage/gpfs_ams/ams/users/$user/.kerberos/$user.keytab ]; then
    ls -lah /storage/gpfs_ams/ams/users/$user/.kerberos/$user.keytab
    echo kinit $user@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/$user.keytab
    kinit $user@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/$user.keytab
  elif [ -e /storage/gpfs_ams/ams/users/$user/.kerberos/$dbuser.keytab ]; then
    ls -lah /storage/gpfs_ams/ams/users/$user/.kerberos/$dbuser.keytab
    echo kinit $dbuser@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/$dbuser.keytab
    kinit $dbuser@CERN.CH -k -t /storage/gpfs_ams/ams/users/$user/.kerberos/$dbuser.keytab
  else
    for keytab in `ls /storage/gpfs_ams/ams/users/$user/.kerberos/*.keytab`; do
      username=`basename $keytab | sed 's/\.keytab//'`
      echo "Trying" kinit $username@CERN.CH -k -t $keytab
      kinit $username@CERN.CH -k -t $keytab
      klist -s
      test1=$?
      klist -l | grep CERN | grep -v Expire
      test2=$?
      if [ $test1 -gt 0 -o $test2 -gt 0 ] ; then
        echo "No ticket, wrong keytab"
      else
        echo "Got a ticket. Nice..."
        break
      fi
    done
  fi
  WORKDIR=`mktemp -d -p /data job.$user.XXXXXXXXXX`
else
  #echo $filelist >&2
  #ls -lah $filelist >&2
  for keytab in `ls *.keytab`; do
      username=`basename $keytab | sed 's/\.keytab//'`
      echo "Trying" kinit $username@CERN.CH -k -t $keytab
      kinit $username@CERN.CH -k -t $keytab
      klist -s
      test1=$?
      klist -l | grep CERN | grep -v Expire
      test2=$?
      if [ $test1 -gt 0 -o $test2 -gt 0 ] ; then
        echo "No ticket, wrong keytab"
      else
        echo "Got a ticket. Nice..."
        break
      fi
  done
  WORKDIR=`mktemp -d -p ./ job.$user.XXXXXXXXXX`
fi
cd $WORKDIR

#manual io redirection for storing logs
if [ ! -z "$STDINFILENAME" ]; then
  stdinfile=$STDINFILENAME
  if [ -e $stdinfile ]; then
      echo "stdin file: $stdinfile"
      cp $stdinfile stdin.log
  fi
fi
exec 1> stdout.log 2> stderr.log

if [ ! -e $amsvar ]; then
    if [ -e ../`basename $amsvar` ]; then
      amsvar=../`basename $amsvar`
    fi
fi

echo "This is job" $jobhash
echo "Using env file:" $amsvar
echo "wn IP address: " `hostname -I`
uname -a
cat /proc/version

source $amsvar

echo "Generating filelist.txt ..."
#python -c "mystr = '$filelist'; print '\n'.join(mystr.split(' '))" > filelist.txt

tempstr=$filelist
python -c "mydir = '$indir'; mystr = '$tempstr'; print '\n'.join(mydir+ '{0}'.format(i) if mydir.endswith('/') else '/{0}'.format(i) for i in mystr.split(' '))" > filelist.txt
cat filelist.txt

echo "Executable to be used:"
ls -lah `echo $userexe | awk '{print $1}'`

rv=0

queryTries=0
queryStatus=1
echo "Trying to reach DB for status report"
while [ $queryStatus -gt 0 ]; do
  echo mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Running\"}, \$currentDate : {\"startTime\" : true}});" $serverDB
  mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Running\"}, \$currentDate : {\"startTime\" : true}});" $serverDB
  queryStatus=$?
  echo "exit status: " $queryStatus
  sleep 10
  let "queryTries=$queryTries + 1"
  if [ $queryTries -gt 60 ]; then
    exit 1
  fi
done

if [[ $rv == 0 ]]; then
  echo "Ready to call the exe"
  # <<<< Attention users - this is the part you'll find yourself editing. (V.F.) >>>>
  echo "$userexe -i filelist.txt -o $outfile"
  $userexe -i filelist.txt -o $outfile
  # <<<< With some luck it'll be also the only one. (V.F.) >>>>

  rv=$?
fi

copyExec=""
localfile=$outfile
remotefile=$outdir/`basename $outfile`

if [[ $remotefile == root://* ]] || [[ $remotefile == /eos/* ]]; then
  copyExec="xrdcp -f"
  isRemote=1
else
  copyExec="cp -v"
  isRemote=0
fi

if [[ $rv == 0 ]]; then
  copyTries=0
  copyStatus=1
  echo "Trying to copy results..."
  while [ $copyStatus -gt 0 ]; do
    echo $copyExec $localfile $remotefile
    $copyExec $localfile $remotefile

    copyStatus=$?
    if [ $isRemote -lt 1 ]; then
      hash1=`md5sum $localfile | awk '{print $1}'`
      hash2=`md5sum $remotefile | awk '{print $1}'`
      if [ ! -e $remotefile -o $hash1 != $hash2 ]; then
        copyStatus=1
      else
        copyStatus=0
        chmod g+w $remotefile
      fi

    fi

    echo "$copyTries : Copy status: " $copyStatus
    sleep 2m
    let "copyTries=$copyTries + 1"
    if [ $copyTries -gt 60 ]; then
      rv=1
      break
    fi
  done
fi

queryTries=0
queryStatus=1
echo "Trying to reach DB for status report"
if [[ $rv != 0 ]]; then
  while [ $queryStatus -gt 0 ]; do
    echo mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Error\"}});" $serverDB
    mongo -u "$dbuser" -p "$passwd" --eval "db.jobs.updateOne({\"hash\" : \"$jobhash\"}, {\$set : {\"status\" : \"Error\"}});" $serverDB
    queryStatus=$?
    echo "exit status: " $queryStatus
    sleep 10
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
    sleep 10
    let "queryTries=$queryTries + 1"
    if [ $queryTries -gt 60 ]; then
      exit 1
    fi
  done
fi

exename=`echo $userexe | awk '{print $1}'`
logdir=$outdir/logs
if [ $isRemote -gt 0 ]; then
    if [ ! -e $logdir ]; then
	mkdir -p $logdir
    fi
fi
filestotar="stdout.log stderr.log"
if [ -e stdin.log ]; then
  filestotar="stdin.log $filestotar"
fi

echo "Archiving and transferring logs..."
tar -cvf `basename $outfile`.logs.tar $filestotar &> /dev/null

echo "Transferring logs"
echo $copyExec `basename $outfile`.logs.tar $logdir/`basename $outfile`.logs.tar
$copyExec `basename $outfile`.logs.tar $logdir/`basename $outfile`.logs.tar

cd ..
rm -vrf $WORKDIR
rm -vf $stdinfile

exit
