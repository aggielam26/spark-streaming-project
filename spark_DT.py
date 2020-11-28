from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob
from elasticsearch import Elasticsearch



TCP_IP = 'localhost'
TCP_PORT = 9001


def processTweet(tweet):

    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search
    es=Elasticsearch([{'host':'localhost','port':9200}])

    tweetData = tweet.split("::")

    if len(tweetData) > 1:

        rawLocation = tweetData[0]
        state = tweetData[1]
        country = tweetData[2]
        lat = tweetData[3]
        lon = tweetData[4]
        text = tweetData[5]

        # (i) Apply Sentiment analysis in "text"
        sentiment = TextBlob(text).polarity

        # (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation


        # print("\n\n=========================\ntweet: ", tweet)
        # print("Raw location from tweet status: ", rawLocation)
        print("lat: ", lat)
        print("lon: ", lon)
        print("state: ", state)
        print("country: ", country)
        # print("Text: ", text)
        # print("Sentiment: ", sentiment)
        
        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!)
        esDoc = {"sentiment":sentiment}
        es.index(index='tweet-sentiment', doc_type='default', body=esDoc)




# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination() 
