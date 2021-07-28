#!/bin/bash
filename=$1
filename1=$2
while IFS="," read -r key type prev count f
do
	# reading each line
	echo ${key}
	st=`echo $key | sed -e 's/^[[:space:]]*//'`
	echo " $st "
	if grep -i " ${st} " //home/phongnve/Downloads/vietnamese-master/VNESEcorpus.txt; then
		echo "$key","$type","$prev","$count","$f" >> $filename1
		continue
	fi
	if grep -i " ${st} " //home/phongnve/Downloads/vietnamese-master/VNTQcorpus-big.txt; then
	    	echo "$key","$type","$prev","$count","$f" >> $filename1
	    	continue
	fi
	if grep -i " ${st} " //home/phongnve/Downloads/vietnamese-master/VNTQcorpus-small.txt; then
	    	echo "$key","$type","$prev","$count","$f" >> $filename1
	    	continue
	fi
	echo "No no"
done < $filename