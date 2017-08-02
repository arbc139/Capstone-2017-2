#!/bin/bash
noh="nohup"
_py=" python"
code=" pmid_insertor.py"
log=" > log/"
out=".out"
for ((i = $1; i<= $2;++i))
do
        noh="nohup"
        noh+=$_py
        noh+=$code
        noh+=" "
        noh+=$i
        $noh &
        echo $i
	wait
done
~

