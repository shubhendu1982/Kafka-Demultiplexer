# Import KafkaConsumer from Kafka library
from kafka import KafkaProducer

# define producer
producer = KafkaProducer(bootstrap_servers='localhost:29092')

# read data from input.txt
with open("input.txt") as myfile:
     for line in myfile:
        # populate key and value
        record_key, record_value = line.partition(":")[::2] 
        record_value=record_value.strip()
        record_key = int(record_key)

        print("Producing record: {}\t{}".format(record_key, record_value))

        # produce data  topic and publish into into topic: data-input
        producer.send('data-input', value=record_value.encode('UTF-8'), partition=record_key)

print("Produced record...")

# flish the producer
producer.flush()