# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer

import bisect

# Import sys module
import sys

msglist = []

# # intert item in sorted order in the list
# def insert(list, n): 
#     # inset the new element in sorted order
#     bisect.insort(list, n)  
#     return list

def insert(n):
    i = 0
    global msglist
    # Searching for the position   
    for i in range(len(msglist)):    
        if msglist[i] > n:
            index = i
            break
      
    # Inserting n in the list
    msglist = msglist[:i] + [n] + msglist[i:]       

## all your app logic here
def main():
    # Define server with port
    bootstrap_servers = ['localhost:29092']

    # Define topic name from where the message will recieve
    topicName = 'data-input'

    # Initialize consumer variable
    consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
    bootstrap_servers,enable_auto_commit=True)
  
    
    # Read and print message from consumer
    for msg in consumer:
     print("Posting message in Topic Name=%s,Message=%s"%(msg.topic,msg.value.decode('UTF-8')))  
     print("Press ctrl+c to run multiplexer & send all data to data-input in sorted order")  
     insert(int(msg.value))

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
   for index in range(len(msglist)): 
    msg= str(msglist[index]).encode('UTF-8')
    print("Posting message in Topic Name=data-output , Message=%s"%(msg))  
    producer.send('data-output', msg)