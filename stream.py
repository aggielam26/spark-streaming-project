import tweepy
import socket
import re
import emoji
import sys
from geopy.geocoders import Nominatim
# import preprocessor



# Enter your Twitter keys here!!!
ACCESS_TOKEN = "3651834372-1Aa44q8X1URhCiY54CVsnf2HquV5xTSMD8iyIBl"
ACCESS_SECRET = "iKkEcEAMJEEEA5zfzfjGAyl4kNCZnoOHowtBj8aCnDriU"
CONSUMER_KEY = "1rr20nc0fX8hobu3eRW4n4dEm"
CONSUMER_SECRET = "S2ZWxtdOxCCW1Moby4ARRDky9SM3BW8Yetshat8HVHbpOtRI7j"


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

def getState(location):

    try:                # location -> latitude, longitude
        address = geolocator.geocode(location, addressdetails=True)
        return address.raw['address']['state']
    except:             # invalid address
        return 'NA'

def getCountry(location):

    try:                # location -> latitude, longitude
        address = geolocator.geocode(location, addressdetails=True)
        return address.raw['address']['country']
    except:             # invalid address
        return 'NA'

def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and remove
    # Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc

    # remove emojis
    modified = re.sub(emoji.get_emoji_regexp(), r'', tweet)

    # remove urls
    modified = re.sub(r"http\S+", "", modified)

    # remove punctuation
    modified = re.sub(r'[^\w\s]', '', modified)

    
    return modified


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
            lat, lon = getCoordinates(location)
            state = getState(location)
            country = getCountry(location)
            tweetLocation = location + "::" + str(state) + "::" + str(country) + "::" + str(lat) + "::" + str(lon) + "::" + tweet + "\n"
            #print(tweetLocation)
            #print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])
