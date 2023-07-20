#! /bin/bash

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "$0: arguments error:\nUsage: $0 inputDataSet output [local|masterName]"
    exit -1
fi

inputDS=$1
outputDir=$2
if [[ $# -eq 3 ]]; then
    mode=$3
else
    mode=masterunisa
fi

spark-submit --class it.unisa.di.soa.ProvaClasse \
  --master yarn --deploy-mode client --driver-memory 4g \
  --num-executors 8 --executor-memory 27g --executor-cores 7 \
  target/ScalaWordCount-1.0-SNAPSHOT.jar   $inputDS $outputDir $mode

