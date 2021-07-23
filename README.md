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
        NB: it is assumed python compiler is already installed (i have used Python 3.9.6)

        # Produce data in kafka from input.txt
        python producer.py

        # Run multiplexer to read from data-input topic, sort data finally write the data to data-output topic
        python multiplexer.py
        NB: press ctrl+c to send the out put to data-output topic 

        # Verify the sorted output in topic data-output
        docker run --net=host --rm confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:29092 --topic data-output --from-beginning

# Task 3 - Scalable Consumer

    # a) Extend your algorithm from 2a to work concurrently. Each partition should be read by exactly one service.         
        The same algorithm 2)a can be used in order to achive insert item into a list maintaining the order
      
    # b) Extend your implementation from 2b by the functionality described in 2a.
        The below command can be used in order to read from a specified partition of a topic and hence it will be able to scale it self by specifying the command line argument to pass the partition no        
        in the format scalableconsumer.py <partition no>  

            The pogram can be executed parally like below exemple
            python .\scalableconsumer.py 1
            python .\scalableconsumer.py 2
            python .\scalableconsumer.py 3
            python .\scalableconsumer.py 4
            .....
            python .\scalableconsumer.py 9

            which will read the message from a particular partition hence can be treated like seperate consumers


      
