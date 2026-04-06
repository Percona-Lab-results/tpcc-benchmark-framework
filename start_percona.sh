#!/bin/bash

docker run -d \
  --name percona \
  --restart unless-stopped \
  -e MYSQL_ROOT_PASSWORD=rootpassword \
  -e MYSQL_DATABASE=mydb \
  -e MYSQL_USER=myuser \
  -e MYSQL_PASSWORD=mypassword \
  -v /data/percona-8.4:/var/lib/mysql \
  -v "$(dirname "$0")/percona.cnf":/etc/my.cnf.d/percona.cnf:ro \
  --network host \
  percona/percona-server:8.4
