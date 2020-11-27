import tweepy
import socket
import re
import emoji
import sys
from geopy.geocoders import Nominatim
# import preprocessor



# Enter your Twitter keys here!!!
ACCESS_TOKEN = "1215453657040392192-h97VO0OK78JcYjgzO2wmUCAB1N6i4N"
ACCESS_SECRET = "Eyj2N5QHGags6M4iaf4XWAJE404xoY848VEvwbq2znK10"
CONSUMER_KEY = "rdznBMCWAhiwRIoe1sSMF9MZr"
CONSUMER_SECRET = "5nUJxjOImHMSrlax3DQXigvw5eKhuCKWRdUKW59HR5TYuVjhGf"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#covid19' if len(sys.argv) == 1 else sys.argv[1]

TCP_IP = 'localhost'
TCP_PORT = 9001

geolocator = Nominatim(user_agent="stream.py")

def getCoordinates(location):

    try:                # location -> latitude, longitude
        coordinates = geolocator.geocode(location)
        return coordinates.latitude, coordinates.longitude
    except:             # invalid address
        return 999, 999

def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and remove
    # Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    return re.sub(emoji.get_emoji_regexp(), r'', tweet)

def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)

        if (location != None and tweet != None):
            latitude, longitude = getCoordinates(location)
            
            tweetLocation = location + "::" + str(latitude) + "::" + str(longitude) + "::" + tweet + "\n"
            #print(tweetLocation)
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])


