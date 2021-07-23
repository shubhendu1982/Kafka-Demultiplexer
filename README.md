# Kafka-Demultiplexer

# Task 1 - Reproducible Environment - Setup kafka Environment
docker-compose pull

docker-compose up -d

docker run --net=host --rm confluentinc/cp-kafka:latest kafka-topics --list --zookeeper  localhost:22181

# Create topic data-input
docker run --net=host --rm confluentinc/cp-kafka:latest kafka-topics --create --topic data-input --partitions 10 --replication-factor 1 --if-not-exists --zookeeper  localhost:22181

# Create topic data-output
docker run --net=host --rm confluentinc/cp-kafka:latest kafka-topics --create --topic data-output --partitions 1 --replication-factor 1 --if-not-exists --zookeeper  localhost:22181

# Task 2 - Simple Consumer
    # a) Come up with a simple algorithm to read the messages and write them to data-output in the desired way
        # intert item in sorted order in the list            
          bisect.insort(list, n) 
            

    # b) Realize your algorithm from a) in python3 using you Environment from Task 1.

        # Change dir to python dir
        cd .\python\

        # Install kafka-python client  lib 
        pip install kafka-python

        # Produce data in kafka from input.txt
        python producer.py

        # Run multiplexer to read from data-input topic, sort data finally write the data to data-output topic
        python multiplexer.py
        NB: press ctrl+c to send the out put to data-output topic 

        # Verify the sorted output in topic data-output
        docker run --net=host --rm confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:29092 --topic data-output --from-beginning

