puts "=== HammerDB TPC-C Schema Build ==="
puts "Warehouses : 1000"
puts "Build VUs  : 64"

dbset db maria
dbset bm TPC-C

diset connection maria_host    127.0.0.1
diset connection maria_port    3306
diset connection maria_socket  null
diset connection maria_ssl     false

diset tpcc maria_user          root
diset tpcc maria_pass          rootpassword
diset tpcc maria_dbase         tpcc
diset tpcc maria_storage_engine innodb
diset tpcc maria_count_ware    1000
diset tpcc maria_num_vu        64
diset tpcc maria_partition     true
diset tpcc maria_history_pk    false

buildschema
puts "=== Schema build complete ==="
