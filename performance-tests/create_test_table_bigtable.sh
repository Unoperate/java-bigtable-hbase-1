#!/bin/bash
set -euo pipefail

table_name=$1
column_family=$2
project_id=$3
instance_id=$4

args="-project $project_id -instance $instance_id"

if cbt $args ls | grep "$table_name"; then
	cbt $args deletetable "$table_name"
fi
cbt $args createtable "$table_name"
cbt $args createfamily "$table_name" "$column_family"
