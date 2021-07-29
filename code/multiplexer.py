"""
This file is used for reading the data from data-input(all partitions) kafka topic
and write it back in data-output in an ordered fashion 
"""
# Import Kafka libs from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer

import os
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

# input file name to feed data into kafka topic in "<partition no>:<value>" format 
input_file = "input.txt"
#-------------------------------------------------------------------------  

# Get number of messages tobe posted in kafka topic from the input file
def getNumberOfMessages():  
    return sum(1 for line in open(os.path.join(sys.path[0], input_file), "r"))

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

    # Get number of messages tobe posted in kafka topic
    lastOffset = getNumberOfMessages()
    
    # Define server with port and start reading from beginning of the kafka stream
    consumer = KafkaConsumer(input_topic_Name,
    bootstrap_servers=bootstrap_server_addr, auto_offset_reset='earliest')       
    
    count = 0
    # Read and print message from consumer and add in a list - msglist in an ordered manner 
    for msg in consumer:
     print("Collecting message from Topic = %s,Message = %s"%(input_topic_Name,msg.value.decode('UTF-8')))     
     insert(int(msg.value))
     count = count + 1  

     # Break the loop once all messages is consumed
     if count == lastOffset:
            break

# Post messages to kafka data-output topic from msglist
def postMessages():

    # Initialize the producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_server_addr,retries=5)

    # Send the ordered messages to data-output topic
    count =0

    for index in range(len(msglist)): 
       msg= str(msglist[index]).encode('UTF-8')
       print("Posting message in Topic = %s , Message = %s"%(output_topic_Name,msg))  
       producer.send(output_topic_Name, msg)
       count = count + 1

    print("Successfully posted = %s Messages"%(count))

# All app logic here
def main():  
    
     # Retrive messages from kafka data-input topic and store in a list in sorted manner
     getMessages()

     print("------------------------------------------")
     print("sending data to multiplexer...............")
     print("------------------------------------------")

     # Post messages to kafka data-output topic
     postMessages()

if __name__ == "__main__":
    main()    