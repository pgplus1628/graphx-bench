
############## Env
SHOME=/home/zork/d/spark
EXE=$SHOME/bin/spark-submit
JAR=/home/zork/dev-pla/graphx-bench/target/scala-2.10/graphx-bench_2.10-1.0.jar

FS_PATH="/zork/graph"
DATA_PATH="hdfs://192.168.1.58:9000${FS_PATH}"


###############  Spark Config
spk_master="10.0.0.8:6066"
spk_master_cli="10.0.0.8:7077" # spark master in client mode
#spk_master="192.168.1.58:6066"
num_slaves=8
dpl_mode="client" # deploy mode
#dpl_mode="cluster"
log4j_conf="./log4j.properties"


###############  HDFS utils
function hdfs_mkdir { 
  target=$1
  echo `hdfs dfs -mkdir -p $target`
  echo `hdfs dfs -ls $target`
}


############## GraphX Config
dft_par="RandomVertexCut"
bigraph_par="CanonicalRandomVertexCut"

############## run functions


# make options for spark submit, 
# options : app graph num_cores_per_exec d
function make_opts { 
  app=$1
  graph=$2
  exec_cores=$3
  d=$4
  if [ $exec_cores == 16 ] ; then 
    if [ $dpl_mode == "cluster" ] ; then
      tot_cores=$(($num_slaves*$exec_cores-1))
    else 
      tot_cores=$(($num_slaves*$exec_cores))
    fi
  else 
    tot_cores=$(($num_slaves*$exec_cores))
  fi

  data_in=${DATA_PATH}/${graph}
  fs_out="${app}_out/`basename ${graph}`-$exec_cores"
  #hdfs_mkdir $fs_out
  data_out=${DATA_PATH}/${fs_out}

  comm_opts="--class org.zork.graphx.BenchMain"
  if [ $dpl_mode == "cluster" ] ; then
    comm_opts="${comm_opts} --master spark://${spk_master}"
  else 
    comm_opts="${comm_opts} --master spark://${spk_master_cli}"
  fi
  comm_opts="${comm_opts} --deploy-mode ${dpl_mode} "
  
  # executor config
  comm_opts="${comm_opts} --executor-cores ${exec_cores}"
  comm_opts="${comm_opts} --total-executor-cores ${tot_cores}"

  # logging
  #comm_opts="${comm_opts} --files ${log4j_conf}"

  # partition 
  app_opts="${app_opts} $JAR ${app} ${data_in} --output=${data_out} --numEPart=-1"
  #app_opts="${app_opts} --partStrategy=${dft_par}"

  # app config
  is_bigraph=0
  if [ $app == "pagerank" ] ; then
    is_bigraph=0
  elif [ $app == "trustrank" ] ; then
    is_bigraph=0
  elif [ $app == "als" ] ; then
    is_bigraph=1
  elif [ $app == "sgd" ] ; then
    is_bigraph=1
  elif [ $app == "svdpp" ] ; then 
    is_bigraph=1
  else 
    echo "unknow app $app"
    exit 1
  fi

  if [ $is_bigraph == 0 ] ; then
    app_opts="${app_opts} --numIter=2"
    app_opts="${app_opts} --partStrategy=${dft_par}"
  else 
    app_opts="${app_opts} --numIter=2 --d=$d"
    app_opts="${app_opts} --partStrategy=${bigraph_par}"
  fi

  comm_opts="${comm_opts} ${app_opts}"
  echo "${comm_opts}"
}



# launch application with spark submit
# options : app graph num_cores_per_exec d
function launch { 
  app=$1
  graph=$2
  exec_cores=$3
  d=$4

  opts=$(make_opts $app $graph $exec_cores $d)
  echo "++++++++++++++++++++++++++++++++++++++++"
  echo " launch $app $graph $exec_cores with option : $opts "
  echo "----------------------------------------"

  ${EXE} $opts
}


####### Launch Main
# options : app graph num_cores_per_exec d
# default number of nodes : 8

launch $1 $2 $3 $4

#launch "pagerank" "live/soc-LiveJournal1.txt" 16


