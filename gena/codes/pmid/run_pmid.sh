#!/bin/bash
noh="nohup"
_py=" python"
code=" get_pmid.py"
log=" > log/"
out=".out"
hold=60
for ((i = $1; i<= $2;++i))
do
	noh="nohup"
	noh+=$_py
	noh+=$code
	noh+=" "
	noh+=$i
	$noh &
	rem=`expr $i % $hold`
	if [ "$rem" == 0 ]; then
		wait
	fi
           echo $i

done
