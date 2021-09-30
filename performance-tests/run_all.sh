duration=10
ops=100
tag=$(date +%s)
docker-compose run test hbase1 $duration $ops $tag
docker-compose run test bigtable1 $duration $ops $tag
docker-compose run test mirroring1-hbase-to-bigtable $duration $ops $tag
docker-compose run test mirroring1-bigtable-to-hbase $duration $ops $tag

pr -m -t -w 240 ./results/*${tag}
