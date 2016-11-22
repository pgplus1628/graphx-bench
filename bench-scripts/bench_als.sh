#!/bin/bash

HOME="/home/zork/d/graphx-bench"
EXEC="$HOME/bench-scripts/launch.sh"
LOG_HOME="$HOME/logs"

app=als
LOG_HOME="$LOG_HOME/$app"

mkdir -p ${LOG_HOME}


function do_bench { 
  graph=$1
  np=$2
  d=$3
  t=`date +"%Y%m%d-%H-%M-%S"`
  log_name="${app}-$d-`basename $graph`-$np-$t"
  echo "$t ${app} $graph $np $d"
  log_out=${LOG_HOME}/$log_name
  $EXEC "${app}" $graph $np $d > $log_out
}


#do_bench "nf/nf" 8 8


for graph in "nf/nf"
do
  for np in 1 2 4 8 16
  do
    do_bench $graph $np 8
    sleep 30
  done
done


for graph in "ymusic/ymusic.snap"
do
  for np in 1 2 4 8 16
  do
    do_bench $graph $np 8
    sleep 30
  done
done


