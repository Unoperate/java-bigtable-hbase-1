duration=900
ops=200
tag=$(date +%s)
# ./perftest.sh hbase1 $duration $ops $tag
# ./perftest.sh bigtable1 $duration $ops $tag
./perftest.sh mirroring1-hbase-to-bigtable $duration $ops $tag
# ./perftest.sh mirroring1-bigtable-to-hbase $duration $ops $tag

pr -m -t -w 240 ./results/*${tag}
