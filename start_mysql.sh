#!/bin/bash

docker run -d \
  --name mysql \
  --restart unless-stopped \
  -e MYSQL_ROOT_PASSWORD=rootpassword \
  -e MYSQL_DATABASE=mydb \
  -e MYSQL_USER=myuser \
  -e MYSQL_PASSWORD=mypassword \
  -v /data/mysql-8.4:/var/lib/mysql \
  -v "$(dirname "$0")/mysql.cnf":/etc/mysql/conf.d/mysql.cnf:ro \
  --network host \
  mysql:8.4.8
