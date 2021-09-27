#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer


kafkaconsumer = KafkaConsumer(
                        bootstrap_servers='localhost:9092',
                        auto_offset_reset='earliest',
                        group_id='HalloweenTwitter',
                        consumer_timeout_ms=2000)

consumer.subscribe('halloween')


for tweet in kafkaconsumer:
    print(tweet)

