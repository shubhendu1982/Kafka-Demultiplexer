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

def insert(n):    
    global msglist 
    i= 0
    # Searching for the position   
    for i in range(len(msglist)):    
        if msglist[i] > n:
            index = i
            break
        else: i = -1
     
    if i!= -1:
        # Inserting n in the list
        msglist = msglist[:i] + [n] + msglist[i:]          
    else:
        msglist.append(n)

## all your app logic here
def main():  
    # Define server with port 

    # Initialize consumer variable
    consumer = KafkaConsumer(input_topic_Name,
    bootstrap_servers=bootstrap_server_addr, auto_offset_reset='earliest')       
    
    # Read and print message from consumer and add in a list in an ordered manner 
    for msg in consumer:
     print("Collecting message from Topic = %s,Message = %s"%(msg.topic,msg.value.decode('UTF-8')))  
     print("")  
     print("Press ctrl+c to run multiplexer & send all data to %s in sorted order"%(output_topic_Name)) 
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