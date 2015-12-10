
############## Env
SHOME=/home/zork/d/spark
EXE=$SHOME/bin/spark-submit
JAR=/home/zork/dev-pla/graphx-bench/target/scala-2.10/graphx-bench_2.10-1.0.jar

FS_PATH="/zork/graph"
DATA_PATH="hdfs://192.168.1.58:9000${FS_PATH}"


###############  Spark Config
spk_master="10.0.0.8:7077"



###############  HDFS utils
function hdfs_mkdir { 
  target=$1
  echo `hdfs dfs -mkdir -p $target`
  echo `hdfs dfs -ls $target`
}


############## GraphX Config
dft_par="RandomVertexCut"


############## run functions


# make options for spark submit, 
# options : app graph
function make_opts { 
  app=$1
  graph=$2

  data_in=${DATA_PATH}/${graph}
  fs_out="${app}_out/`basename ${graph}`"
  hdfs_mkdir $fs_out
  data_out=${DATA_PATH}/${fs_out}

  comm_opts="--class org.zork.graphx.BenchMain"
  comm_opts="${comm_opts} --master spark://${spk_master}"
  comm_opts="${comm_opts} --deploy-mode cluster "
  comm_opts="${comm_opts} $JAR ${app} ${data_in} --output=${data_out} --numEPart=8 --numIter=10 "

  # partition strategy
  comm_opts="${comm_opts} --partStrategy=${dft_par}"

  echo "${comm_opts}"
}



# launch application with spark submit
# options : app graph
function launch { 
  app=$1
  graph=$2
  opts=$(make_opts $app $graph)
  echo "++++++++++++++++++++++++++++++++++++++++"
  echo " launch $app $graph with option : $opts "
  echo "----------------------------------------"

  ${EXE} $opts
}


####### Launch 


launch "pagerank" "live/soc-LiveJournal1.txt"


