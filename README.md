# Geometric Streams Monitoring API for Flink

##### Run on Flink cluster:
  1) Start Flink Cluster and Kafka Server
  
  2) Create kafka topics for input and feedback: 
        ```
        $/path/to/kafka/.../bin/kafka-topics.sh --create --topic <name> --zookeeper <host:port> --partitions <num of workers> --replication-factor <kafka servers>
        ```
  3) Populate input topic:
        ```
        $/path/to/kafka/.../bin/kafka-console-producer.sh --broker-list <host:port,...> --topic <input-topic-name> < /path/to/dataset/file.txt
        ```  
    
  4) Create a .properties file with the following attributes:
        ```input-topic=input-testset-day46     // kafka input topic
        feedback-topic=feedback             // kafka topic for iteration
        output=logs/output.txt              // path to output file
        kafka-servers=localhost:9092        // kafka broker list
        parallelism=6                       // job parallelism
        jobName=fgm-windowless              // job name
        sliding-window=false                // sliding-window switch
        rebalance=false                     // fgm rebalance switch
        window=1600                         // window size (sec)
        slide=5                             // window slide (sec)
        warmup=5                            // system startup delay
        workers=10                          // k (# distributed nodes)
        epsilon=0.1                         // fgm error
        ```
  5) Execute the following command:
        ```
        $/path/to/flink/.../bin/flink run -c MonitoringJobWithKafka \
            /path/to/jar/.../target/streams.monitoring-1.0-SNAPSHOT.jar \
            /path/to/properties/.../file.properties
        ```

##### Goals:
  * Add a "Monitoring" operator on Flink
  * Implement FGM + rebalancing + optimizer
  * Implement Fast AGMS sketches (available at http://github.com/edwardep/AGMS-safezones)
  
#### Work in progress
