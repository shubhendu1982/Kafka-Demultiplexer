# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer

import bisect

# Import sys module
import sys

msglist = []

# intert item in sorted order in the list
def insert(list, n): 
    # inset the new element in sorted order
    bisect.insort(list, n)  
    return list

## all your app logic here
def main():
   ## whatever your app does.
    # Define server with port
    bootstrap_servers = ['localhost:29092']

    # Define topic name from where the message will recieve
    topicName = 'data-input'

    # Initialize consumer variable
    consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
    bootstrap_servers)

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