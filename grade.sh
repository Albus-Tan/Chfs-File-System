#!/bin/bash
make clean &> /dev/null 2>&1
echo "Building lab4 tests"
make &> /dev/null 2>&1 

./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1

score=0

mkdir chfs1 >/dev/null 2>&1

./start.sh

mkdir -p extfs >/dev/null 2>&1

test_if_has_mount(){
	mount | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: Your CHFS client has failed to mount its filesystem!"
			exit
	fi;
	chfs_count=$(ps -e | grep -o "chfs_client" | wc -l)
	extent_count=$(ps -e | grep -o "extent_server_d" | wc -l)

	if [ $chfs_count -ne 1 ];
	then
			echo "error: chfs_client not found (expecting 1)"
			exit
	fi;

	if [ $extent_count -ne 1 ];
	then
			echo "error: extent_server_dist not found"
			exit
	fi;
}
test_if_has_mount

##################################################

wc_test(){
./test-lab4-a.sh extfs | grep -q "Passed mr-wc test."
if [ $? -ne 0 ];
then
        echo "Failed test-a"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+30))
			echo "Passed part A (Word Count)"
			#echo $score
		fi
fi
}

wc_test

##################################################

mr_wc_test(){
./test-lab4-b.sh extfs | grep -q "Passed mr-wc-distributed test."
if [ $? -ne 0 ];
then
        echo "Failed test-part2-b"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+50))
			echo "Passed part B-a (Word Count with distributed MapReduce)"
			#echo $score
		fi
fi
}

mr_wc_test

#################################################

mr_wc_test-perf(){
./test-lab4-b.sh chfs1 | grep -q "Passed mr-wc-distributed test."
if [ $? -ne 0 ];
then
        echo "Failed test-part2-b with performance requirements"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+20))
			echo "Passed part B-b (Word Count with distributed MapReduce with performance requirements)"
			#echo $score
		fi
fi
}

mr_wc_test-perf
# finally reaches here!
if [ $score -eq 100 ];
then
	echo "Lab4 passed"
	echo "Passed all tests!"
else
	echo "Lab4 failed"
fi

./stop.sh >/dev/null 2>&1
rm -rf extfs/

echo ""
echo "Score: "$score"/100"
