#!/bin/bash

mkdir -p /data/seekdb

docker run -d \
  --name seekdb \
  --restart unless-stopped \
  -e MEMORY_LIMIT=32G \
  -e LOG_DISK_SIZE=32G \
  -e CPU_COUNT=0 \
  -e DATAFILE_MAXSIZE=512G \
  -e ROOT_PASSWORD=password \
  -e SEEKDB_DATABASE=sbtest \
  -v /data/seekdb:/var/lib/oceanbase \
  --network host \
  oceanbase/seekdb:1.2.0.0-100000222026032420
