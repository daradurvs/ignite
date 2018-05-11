#!/bin/bash
# The project should be compiled first according to DEVNOTES.txt
# 
# Script should be executed in: ignite/modules/core
# 
# Command to run script: 'nohup ./coverage.sh >/dev/null 2>&1 &'
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

echo "***** Started."

# echo "***** Cleaning..."

# mvn clean

echo "***** Finding tests classes..."

tests=()

while IFS=  read -r -d $'\0'; do
    tests+=("$REPLY")
done < <(find $SCRIPT_DIR/src/test/java/org/apache/ignite -type f -name "*Test*" ! -name "*\$*" ! -name "*Abstract*" ! -name "*TestSuite*" -print0)

testsCount=${#tests[@]}

echo "***** Tests classes number="$testsCount

idx=0

for path in ${tests[@]}
do
	idx=$((idx+1))
	echo "***** Run "$idx" of "$testsCount
	echo "***** Working with: "$path

	filename=$(basename -- "$path")
	filename="${filename%.*}"

	echo "***** Class name: "$filename

	runDir=$SCRIPT_DIR"/results/"$filename
	
	mkdir -p $runDir
	
	if [ "$(ls -A $runDir)" ]; then
		echo "***** Results directory is not empty, skipping..."
		continue
	fi
	
	echo "***** Running tests..."
	
	timeout 30m mvn -P surefire-fork-count-1,coverage test -Dmaven.main.skip=true -Dmaven.test.failure.ignore=true -Dtest=$filename -DfailIFNoTests=false -DrunDirectory=$runDir

	echo "***** Cleaning environment..."
	
	pkill java
done

# mvn -X -P surefire-fork-count-1,coverage validate

echo "***** Finished."
