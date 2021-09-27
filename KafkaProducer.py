#!/usr/bin/env python
# coding: utf-8

# In[1]:


import tweepy as tw
from kafka import KafkaProducer, KafkaConsumer
import sys

access_token = "1441521688252870658-hVfXFAbQuFTjq2aRGOTj2GITCJ6ygs"
access_token_secret =  "Ur9Cb00Zhe5cqPZQSvlYGtvMdEFuxzRTLCrqFltcPFPfY"
consumer_key =  "KUZjtvvQ50Z7WHpeFyP0ZfJpS"
consumer_secret =  "BiE6b6zrcurxWE85rOOo84IYyxtu5QQJ1Kns0Slbz5RkRjtvhA"

topic = 'halloween'

auth = tw.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tw.API(auth)

kafkaproducer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0, 10, 1),
              value_serializer=lambda x: dumps(x).encode('utf-8'))

def get_HalloweenTweets_data():
    result = api.search("Halloween")
    for i in result:
        record = str(i.text)
        kafkaproducer.send(topic,str.encode(record))

get_HalloweenTweets_data()

def stream_HalloweenTweets_data(time):
    while True:
        get_HalloweenTweets_data()
       # time.sleep(time)

stream_HalloweenTweets_data(60*0.1)


# In[ ]:




