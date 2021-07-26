"""
This file used for producing the data as per input.txt file into data-input kafka topic 
"""

# Import KafkaConsumer from Kafka library
from kafka import KafkaProducer

import sys
import os

# Configurations we can move this to a config file depending upon the requirement
#-------------------------------------------------------------------------
# Kafka server url
bootstrap_server_addr =  'localhost:29092'

# Set input topic name
input_topic_Name = 'data-input'

# input file name to feed data into kafka topic in "<partition no>:<value>" format  
input_file = "input.txt"
#--------------------------------------------------------------------------

# Define producer
producer = KafkaProducer(bootstrap_servers=bootstrap_server_addr,retries=5)

count = 0
# Read data from input.txt in the same directory
with open(os.path.join(sys.path[0], input_file), "r") as myfile:
     for line in myfile:
        # Populate key and value
        record_key, record_value = line.partition(":")[::2] 
        record_value=record_value.strip()
        record_key = int(record_key)

        print("Producing record:  {} in the partitation: {}".format(record_value,record_key))

        # Produce data  topic and publish into into topic: data-input
        producer.send(input_topic_Name, value=record_value.encode('UTF-8'), partition=record_key)
        count = count + 1
        
print("Produced record...%s"%(count)) 

# Flush the producer
producer.flush()