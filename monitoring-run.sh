#!/bin/bash

#on softnet cluster use the directory below
#flink_dir=/usr/local/flink/bin/flink
flink_dir=Downloads/flink-1.10.1/bin/flink

#this is also defined in the pom.xml
entryClass="jobs.MonitoringJob"

#locate the jar file after using 'mvn package', by default located at ./target/streams.monitoring-1.0-SNAPSHOT.jar
jar_location=./target/streams.monitoring-1.0-SNAPSHOT.jar

jobName="fgm_01"

#use file:///{your_file} or hdfs://hostname:port/{your_file}
input="file:///home/edwardep/wc_day46_1.txt"
output="file:///home/edwardep/wc_out.txt"

#window and slides times are in seconds (flink event time)
window=3600
slide=5

#warmup duration is also in seconds (flink processing time)
warmup=5

#flink job parallelism
parallelism=4

#submit and run flink job
${flink_dir} run -c ${entryClass} ${jar_location} \
--input ${input} \
--output ${output} \
--window ${window} \
--slide ${slide} \
--warmup ${warmup} \
--p ${parallelism} \
--jobName ${jobName}
