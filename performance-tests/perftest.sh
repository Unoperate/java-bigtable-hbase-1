#!/bin/bash
set -euo pipefail

echo "ARGS: $@"
echo "JAVA_HOME $JAVA_HOME"
WORK_DIR=$(readlink -f $(dirname "$0"))
if [[ ${#WORK_DIR} -eq 1 ]]; then
	WORK_DIR=""
fi
echo "WORKDIR: $WORK_DIR"

# tagret - hbase1, bigtable1, mirrored1
target=$1

runtime_seconds=$2
ops_per_second=$3

if [[ -z ${4+x} ]]; then 
	tag=$(date +%s)
else
	tag=$4
fi

total_ops=$((runtime_seconds * ops_per_second))

tablename="usertable2"
familyname="family"


config_dir=${WORK_DIR}/configs/$target
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
# sed -i "s/BIGTABLE_CREDENTIALS_FILE/${BIGTABLE_CREDENTIALS_FILE}/g" $config_dir/hbase-site.xml

cp ${WORK_DIR}/workload_template /tmp/workload
sed -i "s/TOTAL_OPS/${total_ops}/g" /tmp/workload


classpath_params=""

if [[ $target =~ "hbase" ]]; then
	echo "Creating table hbase"
	./create_test_table.sh $ZOOKEEPER_QUORUM $ZOOKEEPER_PORT $tablename $familyname
fi
if [[ $target =~ "bigtable" ]]; then
	echo "Creating table bigtable"
	./create_test_table_bigtable.sh $tablename $familyname $BIGTABLE_PROJECT_ID $BIGTABLE_INSTANCE_ID
	classpath_params=${classpath_params}':/bigtable-deps/*'
fi
if [[ $target =~ "mirroring" ]]; then
	classpath_params=${classpath_params}:${WORK_DIR}'/mirroring-deps/*'
fi
echo "Tables created"

echo "${BIGTABLE_EMULATOR_HOST:-no emulator}"

echo "Starting load..."
/ycsb/bin/ycsb load hbase14 -P /tmp/workload -cp ${config_dir}${classpath_params} -p table=$tablename -p columnfamily=family
sleep 5

start_ts=$(date +%s)
result_file="${WORK_DIR}/results/result_${target}_$tag"
echo "$target,$tag" > $result_file
echo "Starting run..."
PROFILE_ARGS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9011 -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.local.only=false"
JVM_ARGS="-jvm-args '$PROFILE_ARGS'"

/usr/bin/time -v /ycsb/bin/ycsb run hbase14 -P /tmp/workload -cp $config_dir${classpath_params} -jvm-args "'""$PROFILE_ARGS""'" -p clientbuffering=false -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=true -p hdrhistogram.output.path=/tmp/hdrhistogram_${target} -p table=$tablename -p columnfamily=family -threads 20 -target $ops_per_second > /tmp/results 2>&1 & 
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

cat /tmp/results | grep -v "mismatch" | grep -v "DBWrapper" | grep -v "SLF4J" | grep -v "log4j" >> $result_file
