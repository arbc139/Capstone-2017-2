#!/bin/bash
noh="nohup"
_py=" python"
code=" hgnc_search.py"
log=" > log/"
out=".out"
hold=30
step=1000
counter=0
for ((i = $1; i<= $2;))
do
	end_i=`expr $i + $step`
	noh="nohup"
	noh+=$_py
	noh+=$code
	noh+=" "
	noh+=$i
	noh+=" "
	noh+=$end_i
	$noh &
	echo $noh
	counter=`expr $counter + 1`
	i=`expr $i + $step`
	rem=`expr $counter % $hold`
	if [ "$rem" == 0 ]; then
		echo "wait"
		wait
	fi
           echo $i
done
