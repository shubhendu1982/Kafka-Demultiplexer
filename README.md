# Kafka-Demultiplexer

# Task 1 - Reproducible Environment - Setup single node kafka cluster environment
     Used docker-compose.yml file in order to setup single node kafka cluster environment. Below are the images used for setting up the environment

     cp-kafka:6.2.0 - Docker Image for Kafka
     cp-zookeeper:6.2.0 - Docker Image for Zookeeper to store kafka metadata

    # 1) Pull Zookeeper and kafka docker image
         docker-compose pull

    # 2) Start the environtment
         docker-compose up -d
         N.B check all services are up and running using command docker-compose logs -f 

    # 3) Create topic with name data-input with 10 partitions
         docker run --net=host --rm confluentinc/cp-kafka:6.2.0 kafka-topics --create --topic data-input --partitions 10 --replication-factor 1 --if-not-exists --zookeeper  localhost:22181

    # 4) Create topic with name data-output with 1 partitions
         docker run --net=host --rm confluentinc/cp-kafka:6.2.0 kafka-topics --create --topic data-output --partitions 1 --replication-factor 1 --if-not-exists --zookeeper  localhost:22181

# Task 2 - Simple Consumer
    # a) Come up with a simple algorithm to read the messages and write them to data-output in the desired way
    
        # Intert "item" in sorted order in the list so that after insertion list remain sorted             
            1) # Searching for the position i, where left side of the array < item and right side > item   
               # If i remains -1 after full iteration "item"" has be appended at the end of the list   

                    for i in range(len(msglist)):    
                        if msglist[i] > item:
                            index = i
                            break
                        else: i = -1                    
        
            2) # Inserting the "item" in the list in the index determined in the previous stage            
                    if i!= -1:                        
                        msglist = msglist[:i] + [item] + msglist[i:]          
                    else:
               # All values in the list are less than or equal to item so we need to appended "item" at the end of the list
                        msglist.append(n)            

    # b) Realize your algorithm from a) in python3 using you Environment from Task 1.
        
         - .\code\producer.py -- used for producing the data as per input.txt file
         - .\code\multiplexer.py - used for reading the data from data-input(all partitions) and write it back in data-output in an ordered fashion 
         - .\code\input.txt - used to provide input to producer.py for data creation       

        # Install kafka-python client lib used to operate on kafka cluster
        pip install kafka-python
        NB: it is assumed python compiler is already installed (i have used Python 3.9.6)

        # Produce data in kafka from input.txt with is in key:value format where key is the partitation no and value is the value to be set in the partitation in data-input topic
        python .\code\producer.py

        # Run multiplexer to read from data-input topic, sort data finally write the data to data-output topic
        python .\code\multiplexer.py         

        # Verify the sorted output in topic data-output
        docker run --net=host --rm confluentinc/cp-kafka:6.2.0 kafka-console-consumer --bootstrap-server localhost:29092 --topic data-output --from-beginning --timeout-ms 5000

# Task 3 - Scalable Consumer

    # a) Extend your algorithm from 2a to work concurrently. Each partition should be read by exactly one service.         
        
        I have usedsame algorithm 2)a in order to achive insert item into a list maintaining the order but this time accepted an extra argument partition no in the commandline.
      
    # b) Extend your implementation from 2b by the functionality described in 2a.
    
        - .\code\producer.py -- used for producing the data as per input.txt file
        - .\code\scalableconsumer.py - used for reading the data from data-input(commandline specified partition) and write it back  in data-output in an ordered fashion 
        - .\code\input.txt - used to provide input to producer.py for data creation

        The below command can be used in order to read from a specified partition of a topic and hence it will be able to scale it self by specifying the command line argument to pass the partition no        
        in the format .\code\scalableconsumer.py <partition no>  

            The pogram can be executed parally like below exemple
            python .\code\scalableconsumer.py 0
            python .\code\scalableconsumer.py 1
            python .\code\scalableconsumer.py 2
            python .\code\scalableconsumer.py 3
            python .\code\scalableconsumer.py 4
            .....
            python .\code\scalableconsumer.py 9

            which will read the message from a particular partition hence can be treated like seperate consumers and can be scaled and run in parallel as per the requirement  
