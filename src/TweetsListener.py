#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import tweepy
import socket
import json


# In[ ]:


# Set up your credentials from http://apps.twitter.com 
consumer_key = ... # **************
consumer_secret = ... # **************
access_token = ... # **************
access_secret = ... # **************
bearer_token = ... # **************   


class TweetsListener(tweepy.StreamingClient):

    def __init__(self, bearer_token, csocket):
        super().__init__(bearer_token)
        self.client_socket = csocket

    def on_tweet(self, tweet):
        try:
            print(tweet.text.encode('utf-8'))
            self.client_socket.send(tweet.text.encode('utf-8'))
        except BaseException as e:
            print("Error on_data: %s" % str(e))



# In[ ]:


def sendData(c_socket):
    twitter_stream = TweetsListener(bearer_token, c_socket)
    twitter_stream.add_rules(tweepy.StreamRule("ether"))  # Add your rules
    twitter_stream.filter()  # Start streaming

if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "127.0.0.1"  # Get local machine name
    port = 5554  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)


# In[ ]:




