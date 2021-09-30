#!/bin/bash
set -euo pipefail
rm -f *.jar
target=$1
# "shaded" 1
# "hadoop" 2
dir=$(dirname $(readlink -f $0))
cd $dir
prefix="migration-tools/mirroring-client"
modules="$prefix/bigtable-hbase-mirroring-client-core-parent/protobuf-java-format-shaded,$prefix/bigtable-hbase-mirroring-client-core-parent/bigtable-hbase-mirroring-client-core"
modules="$modules,$prefix/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x"
if [[ $target -eq 1 ]]; then
  suffix="-shaded"
fi
if [[ $target -ge 2 ]]; then
  suffix="-hadoop"
fi

cd ../mirroring-deps-downloading
rm -rf target
mvn -f pom${suffix}.xml dependency:copy-dependencies
cd ../mirroring-deps
cp ../mirroring-deps-downloading/target/dependency/*.jar .

path="../../$prefix/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x"
cp $path${suffix}/target/bigtable-hbase-mirroring-client-1.x${suffix}-2.0.0-alpha2-SNAPSHOT.jar "$dir"
