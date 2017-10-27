#!/bin/bash

if [ -z $1 ]; then
    echo "No build specified"
    exit
fi

build=$1

grouping=1
if [ ! -z "$2" ]; then
    grouping=$2
fi

eos=/afs/cern.ch/project/eos/installation/0.3.15/bin/eos.select
echo "eos =" $eos

for dir in `$eos ls /eos/ams/MC/AMS02/2014/$build`; do
    echo "Processing $build/$dir"

    outdir=.filelists/$build/$dir
    if [ ! -e $outdir ]; then
        mkdir -p $outdir
    fi

    outfile=.filelists/$build/$dir.txt

    if [ -e $outfile ]; then
        rm $outfile
    fi

    itemp=0
	procfiles=0

    declare -a files_to_add
    declare -a files

    files=(`$eos ls /eos/ams/MC/AMS02/2014/$build/$dir | grep .root`)
    pos=$(( ${#files[*]} - 1 ))
	echo "$dir/$dir - $pos files"
    # for file in `$eos ls /eos/ams/MC/AMS02/2014/$build/$dir | grep .root`; do
    #     echo "root://eosams.cern.ch//eos/ams/MC/AMS02/2014/$build/$dir/$file" >> $outfile
    # done
    for file in "${files[@]}"; do
        files_to_add[$itemp]=$file
        let itemp=$itemp+1
        if [ $itemp -eq $grouping -o $procfiles -eq $pos ]; then
            startrun=`echo ${files_to_add[0]} | cut -d '.' -f 1`
            thisfile=$startrun.txt
            if [ $(($grouping > 1)) ]; then
                endrun=`echo ${files_to_add[$(($itemp -1))]} | cut -d '.' -f 1`
                thisfile=$startrun\_$endrun.txt
            fi
            echo "echo Processing file $thisfile; printf '%s\n' ${files_to_add[@]} > $outdir/$thisfile" >> .commands.txt
    	    itemp=0
    	    files_to_add=()
        fi
        let procfiles=$procfiles+1
    done
    cat .commands.txt | parallel

    find $outdir/ -name \*.txt | sort > .filelists/$build/$dir.txt
done
