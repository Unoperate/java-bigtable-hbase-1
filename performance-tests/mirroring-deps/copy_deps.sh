#!/bin/bash
set -euo pipefail
dir=$(dirname $(readlink -f $0))
cd $dir/../../bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x/
mvn dependency:copy-dependencies -DoutputDirectory="$dir"
cp target/bigtable-hbase-mirroring-client-1.x-2.0.0-alpha2-SNAPSHOT.jar "$dir"
