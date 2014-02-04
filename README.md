liquidm's stable master [druid](https://github.com/metamx/druid)
=================================================================

   - hadoop 1.1.1
   - unneeded modules removed
   - tests disabled in build.sh

kafka-local druid realtime
==========================
   - you will be bandwidth bound
   - put kafka and realtime on the same box
   - 100mb/s on eth0, 10mb/s on lo0, 15gb RAM, load 2.5

kafka partitions
================
it's a bag of hurts, use kafka-repartion.rb to generate an optimal
partition layout

WARNING: cluster config hardcoded, adjust before using.

 ```bash
./kafka-repartion.rb topic
~kafka/bin/kafka-reassign-partitions.sh --zookeeper zk1.dw.lqm.io:2181/kafka --manual-assignment-json-file ./topic.all.json
```
