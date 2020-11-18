#!/bin/bash

filelist=$(ls *.sh *.txt | grep -v $0)
srcpath=`realpath .`

pushd $1

for filename in $filelist
do
	echo $filename

	if [ -L $filename ] ; then
		rm $filename
	fi

	ln -s ${srcpath}/$filename $filename
done

popd
