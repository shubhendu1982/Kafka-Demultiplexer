"""
This file is used for reading the data from from a specified partition passed as command line
from data-input topic and write it back in data-output in an ordered fashion 
It will read the message from a particular partition hence can be treated like 
seperate consumers and can be scaled and run in parallel as per the requirement  
"""

# Import Kafka libs from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition

# Import sys module
import sys

# Global messge array holds temporarily the messages from kafka topic 
msglist = []

# Configurations we can move this to a config file depending upon the requirement
#-------------------------------------------------------------------------
# Kafka server url
bootstrap_server_addr =  'localhost:29092'

# Set input & output topic name
input_topic_Name = 'data-input'
output_topic_Name = 'data-output'
#-------------------------------------------------------------------------

# Validate total arguments
n = len(sys.argv)
if(n!=2) :
   print("Partition id needs to be passed")
   sys.exit()

# Check supplied partition no for integer
if(not sys.argv[1].isdigit()):
    print("Invalid Partition id it needs to be an integer value")
    sys.exit()

partition_no = int(sys.argv[1])

# Print partition no
print("Partitation Id=%s"%(partition_no))     

# Insert item in msglist so that it remain sorted after insertation
def insert(n):    
    global msglist 
    i= 0
    # Searching for the position i, where left side of the array <n and right side >n   
    # If i remains -1 after full iteration it has be appended at the end of the list 
    for i in range(len(msglist)):    
        if msglist[i] > n:
            index = i
            break
        else: i = -1
     
    # Inserting n in the list in i-th pisition 
    if i!= -1:        
        msglist = msglist[:i] + [n] + msglist[i:]          
    else:
    # All values in the list are less than or equal to n so we need to appended n at the end of the list 
        msglist.append(n)

# Get all messages from data-input topic in put in a global list msglist
def getMessages(): 
    # Define server with port     
    consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_server_addr, auto_offset_reset='earliest')
   
    # Initialize partition
    tp = TopicPartition(input_topic_Name, partition_no)
    # Read the specified partition
    consumer.assign([tp])   

    # Obtain the last offset value
    consumer.seek_to_end(tp)
    lastOffset = consumer.position(tp)
    consumer.seek_to_beginning(tp)
    
    # Read and print message from consumer and add in a list in an ordered manner 
    for msg in consumer:
     print("Collecting message from Topic = %s,Message = %s, Partitation = %s"%(msg.topic,msg.value.decode('UTF-8'),partition_no))  
     insert(int(msg.value))

    # Break the loop once all messages is consumed
     if msg.offset == lastOffset - 1:
            break
# Post messages to kafka data-output topic
def postMessages():

   # Initialize the producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_server_addr,retries=5) 

    count = 0

    # Send the ordered messages to data-output topic
    for index in range(len(msglist)): 
      msg= str(msglist[index]).encode('UTF-8')
      print("Posting message in Topic = %s , Message = %s"%(output_topic_Name,msg))  
      producer.send(output_topic_Name, msg)
      count = count + 1

    print("Successfully posted = %s Messages"%(count)) 

## All app logic here
def main():  
    
    # Retrive messages from kafka data-input topic for spefied partition and store in a list in sorted manner
    getMessages()

    print("------------------------------------------")
    print("sending data to multiplexer...............")
    print("------------------------------------------")    

    # Post messages to kafka data-output topic
    postMessages()

if __name__ == "__main__":  
   main()     