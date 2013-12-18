#!/bin/bash

DIR0=test
DIR1=test-parsed
DIR2=test-ingress
DIR3=test-vmap
DIR4=test-graph
PARAM1=dict/dict-corncob.txt

JAR=build/jar/graphbuilder.jar
PRE=com.intel.hadoop.graphbuilder
CLASS=preProc.feature.WikiLDAdatasetJob

##ingress Params
numProc=1
ingress="oblivious"

hadoop dfs -rmr $DIR1 $DIR2 $DIR3 $DIR4

echo "========================Creating Graph==================="
$HADOOP_HOME/bin/hadoop jar $JAR $PRE.$CLASS $DIR0 $DIR1 $PARAM1

#echo "=======================Ingress==========================="
#$HADOOP_HOME/bin/hadoop jar $JAR $PRE.test.LDA.LDAIngressMR $DIR1 $DIR2 $numProc $ingress
#
#echo "======================V record Map========================"
#$HADOOP_HOME/bin/hadoop jar $JAR $PRE.test.LDA.LDAVrecordMR $numProc $DIR2 $DIR3
#
#hadoop dfs -mkdir $DIR4/graph $DIR4/vrecord
#hadoop dfs -mv $DIR2/graph* $DIR4/graph/
#hadoop dfs -mv $DIR3/vdata* $DIR4/vrecord/
