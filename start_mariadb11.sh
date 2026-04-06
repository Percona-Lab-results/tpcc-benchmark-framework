#!/bin/bash

docker run -d \
  --name mariadb \
  --restart unless-stopped \
  -e MYSQL_ROOT_PASSWORD=rootpassword \
  -e MYSQL_DATABASE=mydb \
  -e MYSQL_USER=myuser \
  -e MYSQL_PASSWORD=mypassword \
  -v /data/mariadb-11:/var/lib/mysql \
  -v "$(dirname "$0")/mariadb.cnf":/etc/mysql/conf.d/mariadb.cnf:ro \
  --network host \
  mariadb:11.8.6

