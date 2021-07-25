"""
This file used for producing the data as per input.txt file into data-input kafka topic 
"""

# Import KafkaConsumer from Kafka library
from kafka import KafkaProducer

# define producer
producer = KafkaProducer(bootstrap_servers='localhost:29092',retries=5)

count = 0
# read data from input.txt
with open("input.txt") as myfile:
     for line in myfile:
        # populate key and value
        record_key, record_value = line.partition(":")[::2] 
        record_value=record_value.strip()
        record_key = int(record_key)

        print("Producing record:  {} in the partitation: {}".format(record_value,record_key))

        # produce data  topic and publish into into topic: data-input
        producer.send('data-input', value=record_value.encode('UTF-8'), partition=record_key)
        count = count + 1
        
print("Produced record...%s"%(count)) 

# flush the producer
producer.flush()