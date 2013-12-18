nodes="vivasvat gb1 gb2 gb3 gb4 gb5 gb6 gb7"
LOG_HOME=$HOME/log

for node in $nodes
do
rsh $node "cd $LOG_HOME; ./stop.sh" 
done
