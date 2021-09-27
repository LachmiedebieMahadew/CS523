hbase shell 
# Creating  HalloweenTweets table in HBASE 
create 'HalloweenTweets', 'tweet'

# get rows for table HalloweenTweets
scan 'HalloweenTweets'

# start hbase thrift
hbase thrift start 

export PATH=$PATH:/usr/hdp/current/kafka-broker/bin

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic halloween

video url 
https://web.microsoftstream.com/video/31335e91-144b-4d6a-a0ef-16e49343dd36