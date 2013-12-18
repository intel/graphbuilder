#!/bin/bash
rm logging
nodes="localhost gb1 gb2 gb3 gb4 gb5 gb6 gb7"
# nodes="gb3 gb4"
if [ -z $HADOOP_HOME ]
then
  HADOOP_HOME=$HOME/Hadoop/hadoop-1.0.1
fi
LOG_HOME=$HOME/log
TOTAL_ARTICLE=4003417
TOTAL_VERTES=4299667
TOTAL_PAIR=478113000
TOTAL_ING_S=64

articles=0
pair=0

ingress_start=0
ingress_edges=0
ingress_end=0

finalize_start=0
finalize_edges=0
finalize_end=0

vrec_start=0
mirror_vrecs=0;
vrec_end=0


map=0
reduce=0

interval=$1
numproc=32

if [ -z $1 ]
then
  interval=3
fi 

rm debug

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


cpu=""
net=""
disk=""
mem=""

I0=`date +%s`
I=0
while [[ 1 ]]; do

jobid=`$HADOOP_HOME/bin/hadoop job -list | grep job_ | cut -d"	" -f1`
# echo $jobid
if [ "$jobid" != "" ]; then 
	#Assuming only one job is running at any given point in time
	$HADOOP_HOME/bin/hadoop job -status $jobid > .tmp 

	# parsing
	temp=`grep -w ARTICLE .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then articles=$temp; fi
	# graph create
	# old_pair=$pair
	temp=`grep -w PAIR .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then 
	    pair=$temp
	    # delta_pair=`echo $pair - $old_pair | bc`
        fi
	
  	# LDA Ingress Phase counters
        temp=`grep INGRESS_START .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then ingress_start=$temp; fi

        # old_ingress_edges=$ingress_edges
   	# temp=`grep INGRESS_EDGES .tmp | cut -d= -f2`
   	# if [ -n "$temp" ]; then 
        # ingress_edges=$temp
        # delta_ingress_edges=`echo $ingress_edges - $old_ingress_edges | bc`
        # fi

        temp=`grep INGRESS_END .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then ingress_end=$temp; fi

 

        temp=`grep FINALIZE_START .tmp | cut -d= -f2`
        if [ -n "$temp" ]; then finalize_start=$temp; fi

        # old_finalize_edges=$finalize_edges
        # temp=`grep FINALIZE_EDGES .tmp | cut -d= -f2`
        # if [ -n "$temp" ]; then 
        #     # delta_finalize_edges=`echo $finalize_edges - $old_finalize_edges | bc`
        #     finalize_edges=$temp
        # fi
 
        temp=`grep FINALIZE_END .tmp | cut -d= -f2`
        if [ -n "$temp" ]; then finalize_end=$temp; fi


	# vrec
	temp=`grep VREC_MIRROR_START .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then vrec_start=$temp; fi

        # old_mirror_vrecs=$mirror_vrecs
 	# temp=`grep MIRROR_VRECS .tmp | cut -d= -f2`
 	# if [ -n "$temp" ]; then 
        # mirror_vrecs=$temp
        # delta_mirror_vrecs=`echo $mirror_vrecs - $old_mirror_vrecs | bc`
        # fi

	temp=`grep VREC_MIRROR_END .tmp | cut -d= -f2`
	if [ -n "$temp" ]; then vrec_end=$temp; fi


	#Map and Reduce progress
	temp=`grep "map() completion:" .tmp`
	if [ -n "$temp" ]; then map=${temp:18}; fi

	temp=`grep "reduce() completion:" .tmp`
	if [ -n "$temp" ]; then reduce=${temp:20}; fi

#	echo $articles
#	echo $edges
#	echo $ingress_start
#	echo $ingress_end
#	echo $vrec_start
#	echo $vrec_end

else 
	echo "No jobs running..." 
fi

id=0
cpu_util=""
net_util=""
disk_util=""
mem_util=""
for node in $nodes
do
	temp=`rsh $node "cat $LOG_HOME/summary"`
	cpu_util=${cpu_util}",""`echo $temp | awk '{printf("%.2f",$1)}'`"
	net_util=${net_util}",""`echo $temp | awk '{printf("%.2f",$2)}'`"
	disk_util=${disk_util}",""`echo $temp | awk '{printf("%.2f",$3)}'`"
	mem_util=${mem_util}",""`echo $temp | awk '{printf("%.2f",$4)}'`"
done

time=`date +"%T"`


# echo "compute phase, progress"
if [ "$pair" -lt $TOTAL_PAIR ] && [ "$pair" -gt 0 ] && [ "$ingress_start" -eq 0 ]; then
  phase="Preprocessing XML"
  progress=$map
elif [ "$ingress_start" -gt 0 ] && [ "$ingress_end" -lt $numproc ]; then
  phase="Partitioning graphs"
  # progress=$map
  progress=`echo $map*0.5 + $reduce*0.5 | bc`
  progress=`echo $progress | awk '{printf("%2.2f", $1)}'`
  # edge_rate=`echo $delta_ingress_edges / $interval | bc`
  # progress=`echo $ingress_edges / $TOTAL_PAIR | bc -l`
elif [ "$finalize_start" -gt 0 ] && [ "$finalize_end" -lt $numproc ]; then
  phase="Finalizing graphs"
  progress=`echo $finalize_end / $numproc | bc`
  progress=`echo $progress | awk '{printf("%2.2f", $1)}'`

  # edge_rate=`echo $delta_finalize_edges / $interval | bc`
  # progress=`echo $finalize_edges / $TOTAL_PAIR | bc -l`
elif [ "$vrec_start" -gt 0 ] && [ "$vrec_end" -lt $numproc ]; then
  phase="Distributing vertex records"
  # progress=$map
  progress=`echo $map*0.5 + $reduce*0.5 | bc`
  progress=`echo $progress | awk '{printf("%2.2f", $1)}'`

  # edge_rate=`echo $delta_mirror_edges/ $interval | bc`
  # progress=`echo $mirror_vrecs / $TOTAL_VERTS | bc -l`
elif [ "$vrec_end" -ge $numproc ]; then
  phase="Running GraphLab"
  edge_rate=0
  progress=0
else	
  phase="Hadoop"
  edge_rate=0
  progress=0
fi



#############################
## Print json string
#############################

# progress=`echo $progress | awk '{printf("%2.2f", $1)}'`

echo $phase
echo progress: $progress
echo ingress_start: $ingress_start
echo ingress_end: $ingress_end
echo finalize_start: $finalize_start
echo finalize_end: $finalize_end
echo vrec_start: $vrec_start
echo vrec_end: $vrec_end
echo pair: $pair

I=`date +%s`
I=`echo $I - $I0 | bc`
pre="{\n\"phase_name\": \"${phase}\",\n \"progress\": $progress,\n \"sys_metrics\": [\n"
labels="[\"time\", \"n0\", \"n1\", \"n2\", \"n3\", \"n4\", \"n5\", \"n6\", \"n7\"]"

pre_cpu="\t{\"label\": \"CPU\",\n\t \"units\": \"%Utilization\",\n\t \"id\": \"1\",\n\t \"values\": [\n\t  "${labels}
cpu=${cpu}",\n\t  ["${I}","${cpu_util:1}"]"
cpu_json=${pre_cpu}${cpu}"]\n\t}"

pre_net="\t{\"label\": \"Network\",\n\t \"units\": \"%Utilization\",\n\t \"id\": \"2\",\n\t \"values\": [\n\t  "${labels}
net=${net}",\n\t  ["${I}","${net_util:1}"]"
net_json=${pre_net}${net}"]\n\t}"

pre_disk="\t{\"label\": \"Disk\",\n\t \"units\": \"%Utilization\",\n\t \"id\": \"3\",\n\t \"values\": [\n\t  "${labels}
disk=${disk}",\n\t  ["${I}","${disk_util:1}"]"
disk_json=${pre_disk}${disk}"]\n\t}"

pre_mem="\t{\"label\": \"Memory in MB\",\n\t \"units\": \"MB\",\n\t \"id\": \"4\",\n\t \"values\": [\n\t  "${labels}
mem=${mem}",\n\t  ["${I}","${mem_util:1}"]"
mem_json=${pre_mem}${mem}"]\n\t}"
#mem_json=""

#json=${pre}${mem_json}"\n  ]\n}"
json=${pre}${cpu_json}",\n"${net_json}",\n"${disk_json}",\n"${mem_json}"\n  ]\n}"


echo -e $json > tmp 
rm logging
mv tmp logging
echo -e $json >> debug

sleep $interval
done
