#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from __future__ import print_function
 
import os
import sys

import happybase
import datetime
 
os.environ["SPARK_HOME"] = "/usr/spark2.4.3"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")
 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
 
sc = SparkContext(appName = "StreamingTwitterHalloweenAnalysis")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 20)
 
socket_stream = ssc.socketTextStream("127.0.0.1",9999)
 
tweets = socket_stream.window( 20 )
 
hashtags = tweets.flatMap( lambda text: text.split( " " ) ).filter( lambda word: word.lower().startswith("#") ).map( lambda word: ( word.lower() , 1) ).reduceByKey( lambda a,a1:a+a1)
 
halloween_topics_sorted_dstream = hashtags.transform(lambda h:h.sortBy(lambda x:x[0].lower()).sortBy(lambda x:x[1],ascending=False))
 
halloween_topics_sorted_dstream.pprint()
 
conn = happybase.Connection('127.0.0.1',9090)

conn.open()
table_name = 'HalloweenTweets'
table = conn.table(table_name)
print(" Connecting to table " +str(table))
#table.put(str(datetime.datetime.now()), {'tweet:word':str(halloween_topics_sorted_dstream)})


ssc.start()
 
ssc.awaitTermination()


# In[ ]:




