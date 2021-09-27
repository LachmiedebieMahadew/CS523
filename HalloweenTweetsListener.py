#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import socket
import json

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

access_token = "1441521688252870658-hVfXFAbQuFTjq2aRGOTj2GITCJ6ygs"
access_token_secret =  "Ur9Cb00Zhe5cqPZQSvlYGtvMdEFuxzRTLCrqFltcPFPfY"
consumer_key =  "KUZjtvvQ50Z7WHpeFyP0ZfJpS"
consumer_secret =  "BiE6b6zrcurxWE85rOOo84IYyxtu5QQJ1Kns0Slbz5RkRjtvhA"

class HalloweenTweetsListener(StreamListener):
    def __init__(self,clntsocket):
        self.client_socket = clntsocket
    def on_data(self,tweetsdata):
        try:
            tweets = json.loads( tweetsdata )
            print( tweets['text'].encode('utf-8') )
            self.client_socket.send( tweets['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Exception occured : %s" % str(e))
            return True
    def on_error(self, status):
        print(status)
        return True
    
def sendData(clntsocket):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    twitter_stream = Stream(auth, HalloweenTweetsListener(clntsocket))
    twitter_stream.filter(track=['halloween'])
    
s = socket.socket()
host = "127.0.0.1"
port = 9999
s.bind((host, port))
print("Listening on port: %s" % str(port))

s.listen(5)
print( "Before accepting  " )
c, addr = s.accept()
print( "Received request from: "+ str( addr ) )
 
sendData( c )


# In[ ]:




