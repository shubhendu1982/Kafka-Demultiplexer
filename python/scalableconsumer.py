# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition

import bisect

# Import sys module
import sys

msglist = []

# validate total arguments
n = len(sys.argv)
if(n!=2) :
   print("Partition id needs to be passed")
   sys.exit()

# print partition no
print("Partitation Id=%s"%(sys.argv[1]))      

# intert item in sorted order in the list
def insert(list, n): 
    # inset the new element in sorted order
    bisect.insort(list, n)  
    return list

## all your app logic here
def main():  
    # Define server with port
    bootstrap_servers = ['localhost:29092']
   # Define topic name from where the message will recieve
    topicName = 'data-input'

    # Initialize consumer variable
    consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
   
    # Read the specified partition
    consumer.assign([TopicPartition(topicName, int(sys.argv[1]))]) 

    # echo instructions
    print("Press ctrl+c to run multiplexer")     
    
    # Read and print message from consumer
    for msg in consumer:
     print("Topic Name=%s,Message=%s"%(msg.topic,msg.value.decode('UTF-8')))    
     insert(msglist, int(msg.value))

    # Terminate the script
    sys.exit()

if __name__ == "__main__":
   try:
      main()
   except KeyboardInterrupt:
      # do nothing here
      pass
   print("sending data to multiplexer")
  
   producer = KafkaProducer(bootstrap_servers='localhost:29092')
   for x in range(len(msglist)): 
    print(msglist[x]) 
    producer.send('data-output', str(msglist[x]).encode('UTF-8'))