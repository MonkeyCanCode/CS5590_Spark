#!/usr/bin/python3

import sys
import json
import time
import tweepy
import socket

CONSUMER_KEY = '1D9JkqJry8h9NihcS9iHbDbMt'
CONSUMER_SECRET = 'HPjhBFynz7Uq0byXKQtYMSC4dxGGQLHyNN5ucN8eipgSo7DLmM'
ACCESS_TOKEN = '534898951-DTPYyzqsg1gU8EVq1KnYy0ilo3k2hIm3zmRplWDe'
ACCESS_TOKEN_SECRET = 'pAGY6zxXLMa8mncDIn7lZqPu32kyQUcO2bvRoRXO0d4I0'

def validTweet(str_tweet):
    json_tweet = json.loads(str_tweet)
    return False if list(json_tweet.keys())[0] == 'delete' or list(json_tweet.keys())[0] == 'limit' else True

class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        if validTweet(data):
            tweet = json.loads(data)
            self.client_socket.send(tweet["text"].encode('utf-8'))

    def on_error(self, status):
        print(status)

def main():
    global CONSUMER_KEY
    global CONSUMER_SECRET
    global ACCESS_TOKEN
    global ACCESS_TOKEN_SECRET

    # Create socket 
    s = socket.socket()
    host = 'localhost'
    port = 8000
    s.bind((host, port))
    s.listen(3)
    c_scoket, addr = s.accept()
    time.sleep(3)

    # Twitter streaming
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = tweepy.Stream(auth, TwitterStreamListener(c_scoket))
    stream.filter(languages=['en'], track=['spark', 'hadoop', 'python', 'hdfs', 'solr', 'cassandra', 'lucene', 'cloudera', 'sql', 'election', 'cat', 'dog'])

if __name__ == '__main__':
    main()
