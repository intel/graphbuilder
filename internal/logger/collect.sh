#!/bin/bash

nodes="vivasvat gb1 gb2 gb3 gb4 gb5 gb6 gb7"
HADOOP_HOME=$HOME/Hadoop/hadoop-1.0.1
LOG_HOME=$HOME/log
TOTAL_ARTICLE=9448430
TOTAL_EDGES=478113368

cleaningprogress=0
graphprogress=0
interval=$1

if [ -z $1 ]
then
  interval=5
fi 

echo start collecting data on each node at $interval seconds
 
for node in $nodes
do
	rsh $node "cd $LOG_HOME; ./start.sh $interval" 
done

sleep $interval

for node in $nodes
do
	rsh $node "cd $LOG_HOME; ./parse.sh $interval" &
done 

while [[ 1 ]]; do

jobid=`$HADOOP_HOME/bin/hadoop job -list | grep job_ | cut -d"	" -f1`
echo $jobid
if [ "$jobid" != "" ]; then 
	#Assuming only one job is running at any given point in time
	$HADOOP_HOME/bin/hadoop job -status $jobid > .tmp 

	# LDA Document cleaning
	articles=`grep ARTICLE .tmp | cut -d= -f2`
	echo "Articles: $articles"
	# LDA graph construction 
	edges=`grep EDGES .tmp | cut -d= -f2`
	echo "Edges: $edges"

	# LDA Ingress MAP counters
	edges_touched=`grep INGRESS_EDGES .tmp | cut -d= -f2`
	edges_touched0=`grep PROC0_EDGES .tmp | cut -d= -f2`
	edges_touched1=`grep PROC1_EDGES .tmp | cut -d= -f2`
	edges_touched2=`grep PROC2_EDGES .tmp | cut -d= -f2`
	edges_touched3=`grep PROC3_EDGES .tmp | cut -d= -f2`

	verts_touched0=`grep PROC0_VRECS .tmp | cut -d= -f2`
	verts_touched1=`grep PROC1_VRECS .tmp | cut -d= -f2`
	verts_touched2=`grep PROC2_VRECS .tmp | cut -d= -f2`
	verts_touched3=`grep PROC3_VRECS .tmp | cut -d= -f2`

	vertices=`grep INGRESS_VRECS .tmp | cut -d= -f2`

	# # LDA Ingress Reduce counters
	# edgekey=`grep EDGE_REC .tmp | cut -d= -f2`

	# # LDA vrecord Map 

	# # LDA vrecord Reduce 
else 
	echo "No jobs running..." 
fi

output=""
for node in $nodes
do
temp=`rsh $node "cat $LOG_HOME/summary"`
cpu_util=`echo $temp | awk '{print $1}'`
network_util=`echo $temp | awk '{print $2}'`
disk_util=`echo $temp | awk '{print $3}'`
mem_in_mb=`echo $temp | awk '{print $4}'`
output="$output$node $cpu_util $network_util $disk_util $mem_in_mb " 
#echo  $output
done

time=`date +"%T"`

if [ "$jobid" != "" ]; then 
	if [ "$articles" != "" ] && [ "$cleaningprogress" -le 100 ]; then
		cleaningprogress=`echo "scale=2; $articles / $TOTAL_ARTICLE * 100" | bc`
		echo "$time Cleaning Article:$articles progress:$cleaningprogress $output"
	fi

	if [ "$edges" != "" ] && [ "$graphprogress" -le 100 ]; then  
		graphprogress=`echo "scale=2; $edges / $TOTAL_EDGES * 100" | bc`
		echo "$time Graph Edges:$edges progress:$graphprogress $output"
	fi

  if [ "$edge_touched" != "" ]; then  
    echo "$time Ingress Edges:$edge_touched $edges_touched0 $edges_touched1 $edges_touched2 $edges_touched3 $output"
  fi

  if [ "$verts_touched" != "" ]; then  
    echo "$time Ingress Vrecs:$verts_touched $verts_touched0 $verts_touched1 $verts_touched2 $verts_touched3 $output"
  fi


else
	echo "$time NOJOB NA NA $output"
fi

sleep $interval
done
