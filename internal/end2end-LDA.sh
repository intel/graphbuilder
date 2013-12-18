#!/bin/bash

#full,half,quart
input=wikipedia-dump
DIR1=temp-parsed
DIR2=temp-ingress
output=temp-graph
PARAM1=dict/dict-scowl-lower.txt

JAR=graphbuilder/build/jar/graphbuilder.jar
PRE=com.intel.hadoop.graphbuilder
CLASS=preProc.feature.WikiLDAdatasetJob

##ingress Params
# numProc=16
numProc=32
ingress="oblivious"
#ingress="random"

DO_GB=$1
DO_GL=T

if [ -z $DO_GB ]; then 
	DO_GB='T'
fi

if [ $DO_GB == 'T' ]; then
echo "...Hadoop conf for parsing..."
cp /home/user/confHadoop/preProc/mapred-site.xml $HADOOP_HOME/conf
/home/user/work/cpheap.sh

hadoop dfs -rmr $DIR1 
echo "========================Creating dataset==================="
time $HADOOP_HOME/bin/hadoop jar $JAR $PRE.$CLASS $input $DIR1 $PARAM1
 
 echo "...Hadoop conf for Ingress..."
 cp /home/user/confHadoop/ingress/mapred-site.xml $HADOOP_HOME/conf
 /home/user/work/cpheap.sh
 
 echo "=======================Ingress==========================="
 hadoop dfs -rmr $DIR2
 time $HADOOP_HOME/bin/hadoop jar $JAR $PRE.test.LDA.LDAIngressMR $DIR1 $DIR2 $numProc $ingress
 
 $HADOOP_HOME/bin/hadoop dfs -mkdir $DIR2/graph
 $HADOOP_HOME/bin/hadoop dfs -mv $DIR2/graph?* $DIR2/graph/
 $HADOOP_HOME/bin/hadoop dfs -mv $DIR2/edata?* $DIR2/graph/
 $HADOOP_HOME/bin/hadoop dfs -mv $DIR2/vid2lvid?* $DIR2/graph/
 $HADOOP_HOME/bin/hadoop dfs -mkdir $DIR2/vrecord
 $HADOOP_HOME/bin/hadoop dfs -mv $DIR2/part?* $DIR2/vrecord/
 
 $HADOOP_HOME/bin/hadoop dfs -rmr $output
 echo "======================V record Map========================"
 time $HADOOP_HOME/bin/hadoop jar $JAR $PRE.test.LDA.LDAVrecordMR $numProc $DIR2/vrecord $output
 
 $HADOOP_HOME/bin/hadoop dfs -mv $DIR2/graph $output/
 $HADOOP_HOME/bin/hadoop dfs -mkdir $output/vrecord
 $HADOOP_HOME/bin/hadoop dfs -mv $output/vdata?* $output/vrecord
 
 for i in 0 1 2 3 4 5 6 7 
 do
 	$HADOOP_HOME/bin/hadoop dfs -cat $output/part-0000$i.gz | gunzip | cat
 done
 fi
 
 if [ $DO_GL == 'T' ]; then
 echo "===================================================="
 echo "===========GL================"
 
 GLexe=/home/user/GraphLab/GLv2/graphlabapi/release/toolkits/topic_modeling/cgs_lda
 CORPUS=hdfs:///user/user/$output
 D=hdfs:///user/user/$PARAM1
 docDIR=hdfs:///user/user/gl_output/lda_doc
 wordDIR=hdfs:///user/user/gl_output/lda_word
 TK=50
 TOPK=30
 ALPHA=0.5
 BETA=0.1
 CPU=16
 GLcmd="--dictionary $D --doc_dir $docDIR --word_dir $wordDIR --corpus $CORPUS --ntopics $TK --topk $TOPK --alpha $ALPHA --beta $BETA --ncpus $CPU --engine=asynchronous --format json-gzip"
 
 HOSTF=/home/user/machines
 NP=$numProc
 
 MPIcmd="-np $NP -hostfile $HOSTF env CLASSPATH=`hadoop classpath`"
 
 echo "Corpus : $CORPUS"
 echo "Graphlab command: $GLexe $GLcmd"
 
 time mpiexec $MPIcmd $GLexe $GLcmd 
 #$GLexe $GLcmd
 fi

#
