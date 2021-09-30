#!/bin/bash
set -euo pipefail

echo "ARGS: $@"
echo "JAVA_HOME $JAVA_HOME"

# tagret - hbase1, bigtable1, mirrored1
target=$1

runtime_seconds=$2
ops_per_second=$3

if [[ -z $4 ]]; then 
	tag=$(date +%s)
else
	tag=$4
fi

total_ops=$((runtime_seconds * ops_per_second))

tablename="usertable"
familyname="family"


config_dir=/configs/$target
if [[ ! -d $config_dir ]]; then
	echo "not a directory $config_dir"
	exit 1
fi


# fill hbase-site.xml

cp $config_dir/hbase-site.xml.template $config_dir/hbase-site.xml

sed -i "s/ZOOKEEPER_QUORUM/${ZOOKEEPER_QUORUM}/g" $config_dir/hbase-site.xml
sed -i "s/ZOOKEEPER_PORT/${ZOOKEEPER_PORT}/g" $config_dir/hbase-site.xml

sed -i "s/BIGTABLE_PROJECT_ID/${BIGTABLE_PROJECT_ID}/g" $config_dir/hbase-site.xml
sed -i "s/BIGTABLE_INSTANCE_ID/${BIGTABLE_INSTANCE_ID}/g" $config_dir/hbase-site.xml
sed -i "s/BIGTABLE_CREDENTIALS_FILE/${BIGTABLE_CREDENTIALS_FILE}/g" $config_dir/hbase-site.xml

cp /workload_template /workload
sed -i "s/TOTAL_OPS/${total_ops}/g" /workload


classpath_params=""

if [[ $target =~ "hbase" ]]; then
	./create_test_table.sh $ZOOKEEPER_QUORUM $ZOOKEEPER_PORT $tablename $familyname
fi
if [[ $target =~ "bigtable" ]]; then
	./create_test_table_bigtable.sh $tablename $familyname $BIGTABLE_PROJECT_ID $BIGTABLE_INSTANCE_ID
	classpath_params=${classpath_params}':/bigtable-deps/*'
fi
if [[ $target =~ "mirroring" ]]; then
	classpath_params=${classpath_params}':/mirroring-deps/*'
fi

echo "${BIGTABLE_EMULATOR_HOST:-no emulator}"

echo "Starting load..."
/ycsb/bin/ycsb load hbase14 -P /workload -cp ${config_dir}${classpath_params} -p table=$tablename -p columnfamily=family
sleep 5

start_ts=$(date +%s)
result_file="/results/result_${target}_$tag"
echo "$target,$tag" > $result_file
echo "Starting run..."
/ycsb/bin/ycsb run hbase14 -P /workload -cp $config_dir${classpath_params} -p table=$tablename -p columnfamily=family -threads 1 -target $ops_per_second > /tmp/results & 
operation_pid=$!


while ps -p $operation_pid > /dev/null; do
	# verify
	offset_ts=$(( $(date +%s) - start_ts ))

	usage=$(LC_NUMERIC='LC_ALL' ps ao rss,%cpu | awk 'BEGIN {mem=0; cpu=0} {mem+=$1; cpu+=$2} END {print mem, cpu}')
	cpu_usage=$(echo $usage | cut -f2 -d" ")
	used_memory_bytes=$(echo $usage | cut -f1 -d" ")

	echo "$offset_ts,$cpu_usage,$used_memory_bytes" >> $result_file

	sleep 5
done

cat /tmp/results | grep -v "mismatch" >> $result_file
