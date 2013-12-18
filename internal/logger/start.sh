vmstat $1 > cpu &
ifstat -i eth2 $1 > network &
iostat -dx $1 > disk &
