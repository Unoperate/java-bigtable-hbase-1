#!/bin/bash
set -euo pipefail

zookeeper_quorum=$1
zookeeper_port=$2
table_name=$3
column_family=$4

cd /hbase

args="-Dhbase.zookeeper.quorum=${zookeeper_quorum} -Dhbase.zookeeper.client.port=${zookeeper_port}"

if echo "list" | ./bin/hbase shell $args -n | grep "$table_name"; then
	# table exists, truncate
	echo "truncate '$table_name'" | ./bin/hbase shell $args -n
else
	echo "create '$table_name', '$column_family' " | ./bin/hbase shell $args -n
fi
