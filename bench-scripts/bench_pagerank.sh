#!/bin/bash

HOME="/home/zork/d/graphx-bench"
EXEC="$HOME/bench-scripts/launch.sh"
LOG_HOME="$HOME/logs"

app=pagerank
LOG_HOME="$LOG_HOME/$app"

mkdir -p ${LOG_HOME}

for graph in "live/soc-LiveJournal1.txt" "twitter/twitter_rv.net" "uk2007/uk-2007.snap"
do
  for np in 1 2 4 8 16
  do
    t=`date +"%Y%m%d-%H-%M-%S"`
    log_name="pagerank-`basename $graph`-$np-$t"
    echo "$t pagerank $graph $np "
    log_out=${LOG_HOME}/$log_name
    $EXEC "pagerank" $graph $np > $log_out
  done
done

