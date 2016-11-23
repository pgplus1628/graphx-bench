#!/bin/bash

HOME="/home/zork/d/graphx-bench"
EXEC="$HOME/bench-scripts/launch.sh"
LOG_HOME="$HOME/logs"

app=pagerank
LOG_HOME="$LOG_HOME/$app"

mkdir -p ${LOG_HOME}


function do_bench { 
  graph=$1
  np=$2
  t=`date +"%Y%m%d-%H-%M-%S"`
  log_name="pagerank-`basename $graph`-$np-$t"
  echo "$t pagerank $graph $np "
  log_out=${LOG_HOME}/$log_name
  $EXEC "pagerank" $graph $np > $log_out
}


do_bench "live/soc-LiveJournal1.txt" 16
#do_bench "twitter/twitter_rv.net" 16


#for graph in "live/soc-LiveJournal1.txt"
#do
#  for np in 1 2 4 8 16
#  do
#    do_bench $graph $np
#    sleep 30
#  done
#done
#
#
##for graph in "live/soc-LiveJournal1.txt" "twitter/twitter_rv.net" "uk2007/uk-2007.snap"
#
#for graph in "twitter/twitter_rv.net"
#do
#  for np in 1 2 4 8 16
#  do
#    do_bench $graph $np
#    sleep 30
#  done
#done
#
#
#for graph in "uk2007/uk-2007.snap"
#do
#  for np in 1 2 4 8 16
#  do
#    do_bench $graph $np
#    sleep 30
#  done
#done
#
