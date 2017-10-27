#!/bin/bash

eos='/afs/cern.ch/project/eos/installation/0.3.15/bin/eos.select'
echo "eos =" $eos

build=ISS.B950
pass=pass6

if [ ! -z "$1" ]; then
    build=$1
fi
if [ ! -z "$2" ]; then
    pass=$2
fi


$eos ls /eos/ams/Data/AMS02/2014/$build/$pass > runlist.txt


if [ ! -e .filelists/$build/$pass ]; then
    mkdir -p .filelists/$build/$pass
fi

for file in `cat runlist.txt`; do
    run=`basename $file | sed s/.root// | sed s/'\.'/\ / | awk '{print $1}'`;
    if [ -e .filelists/$build/$pass/$run.txt ]; then
	continue;
    fi
    echo $run;
done | parallel "echo Creating {}; grep {} runlist.txt > .filelists/$build/$pass/{.}.txt;"

find .filelists/$build/$pass/ -type f | sort > .filelists/$build/$pass.txt
