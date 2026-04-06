puts "=== HammerDB TPC-C Benchmark Run ==="
puts "Virtual users : 2"
puts "Ramp-up       : 1 min"
puts "Duration      : 60 min"
puts ""

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
diset tpcc maria_history_pk    false
diset tpcc maria_driver        timed
diset tpcc maria_rampup        1
diset tpcc maria_duration      60
diset tpcc maria_timeprofile   true
diset tpcc maria_allwarehouse  true

# 1-second polling loop
proc runtimer { total_secs } {
    set elapsed 0
    set prev_time [clock seconds]
    while { $elapsed < $total_secs } {
        after 1000
        set now [clock seconds]
        incr elapsed [expr { $now - $prev_time }]
        set prev_time $now
        set mins [expr { $elapsed / 60 }]
        set secs [expr { $elapsed % 60 }]
        puts -nonewline "\r  Elapsed: [format %02d $mins]:[format %02d $secs] / [expr { $total_secs / 60 }]:[format %02d [expr { $total_secs % 60 }]]"
        flush stdout
        if { [vucomplete] } { break }
        update
    }
    puts ""
}

tcset refreshrate 1
loadscript

vuset vu 2
vuset logtotemp 1
vucreate

set total_secs [expr { 60 + 3600 + 30 }]
puts "Starting virtual users..."
vurun
runtimer $total_secs

vudestroy
puts "=== Benchmark run complete ==="
