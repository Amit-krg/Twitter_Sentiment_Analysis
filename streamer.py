import sys
import tweepy
import json

consumer_key = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
consumer_secret = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
access_secret = 'xxxxxxxxxxxxxxxxxxxxxxxxx'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

file2 = open('tweets_data.csv', 'a')

class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print status.text

    def on_data(self, data):
        json_data = json.loads(data)

#extract tweet text from json and store to file       
        try:
            line = json_data["text"]
            line = line.encode('utf-8')        
            string1 = str(line)
            file2.write(string1)
            file2.write("\n")
        except KeyError:
            pass

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True 		#Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True 		#Don't kill the stream

#starts the steam and listens continuously
sapi = tweepy.streaming.Stream(auth, CustomStreamListener())

#Filtering Tweets for United States region
sapi.filter(locations=[-124.8,29.78,-67.32,48.81])
