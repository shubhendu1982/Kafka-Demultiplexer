"""
This file is used for reading the data from data-input(all partitions) kafka topic
and write it back in data-output in an ordered fashion 
"""
# Import Kafka libs from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer

# Import sys module
import sys

# global messge array holds temporarily the messages from kafka topic 
msglist = []

# configurations
#-------------------------------------------------------------------------
# set the kafka server url
bootstrap_server_addr =  'localhost:29092'

# set input topic name
input_topic_Name = 'data-input'
output_topic_Name = 'data-output'
#-------------------------------------------------------------------------  
# get number of messages
def getNumberOfMessages():  
    return sum(1 for line in open('input.txt'))

# insert item in msglist so that it remain sorted after insertation
def insert(n):    
    global msglist 
    i= 0
    # Searching for the position   
    for i in range(len(msglist)):    
        if msglist[i] > n:
            index = i
            break
        else: i = -1
     
    # Inserting n in the list
    if i!= -1:        
        msglist = msglist[:i] + [n] + msglist[i:]          
    else:
        msglist.append(n)

# Get all messages from data-input topic in put in a global list msglist
def getMessages(): 
# Define server with port 
    lastOffset = getNumberOfMessages()
    # Initialize consumer variable
    consumer = KafkaConsumer(input_topic_Name,
    bootstrap_servers=bootstrap_server_addr, auto_offset_reset='earliest')       
    
    count = 0
    # Read and print message from consumer and add in a list - msglist in an ordered manner 
    for msg in consumer:
     print("Collecting message from Topic = %s,Message = %s"%(input_topic_Name,msg.value.decode('UTF-8')))     
     insert(int(msg.value))
     count = count + 1     
     # end of message
     if count == lastOffset:
            break

# post messages to kafka data-output topic from msglist
def postMessages():

    # initialize the producer
     producer = KafkaProducer(bootstrap_servers=bootstrap_server_addr,retries=5)

    # send the ordered messages to data-output topic
     count =0
     for index in range(len(msglist)): 
            msg= str(msglist[index]).encode('UTF-8')
            print("Posting message in Topic = %s , Message = %s"%(output_topic_Name,msg))  
            producer.send(output_topic_Name, msg)
            count = count + 1

            print("Successfully posted = %s Messages"%(count))

## all app logic here
def main():  
    
     # Retrive messages from kafka data-input topic and store in a list in sorted manner
     getMessages()

     print("------------------------------------------")
     print("sending data to multiplexer...............")
     print("------------------------------------------")

     # post messages to kafka data-output topic
     postMessages()

if __name__ == "__main__":
    main()    