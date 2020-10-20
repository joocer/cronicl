"""
Pipeline Demo - Live Twitter 
"""

import cronicl
import logging, sys
import networkx as nx
import tweepy
import re

auth = tweepy.OAuthHandler("h7PbJYq3A5KWozZ5FgRSraOJN", "bTONoBB7YdweCO6ul6j4gBF2wlO4OIjKd4wn3wkz9nOQQjETw2")
auth.set_access_token("1000125101004779526-R3xegh8e1E8U2ewL7yazLE09oA8D8f", "uQUwBlRzzcaXmJPwpz2abns1gbZG21gLnkkTLqeO2W1z0")
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")

class ExtractTweetDetailsOperation(cronicl.operations.Operation):

    def execute(self, message):
        tweet = message.payload

        try:
            quoting = tweet.quoted_status_id
        except:
            quoting = None

        retweeting = None
        full_text = None
        if hasattr(tweet, "retweeted_status"):
            try:
                retweeting = tweet.retweeted_status.id
                full_text = tweet.retweeted_status.extended_tweet["full_text"]
            except AttributeError:
                full_text = tweet.retweeted_status.text
        else:
            try:
                full_text = tweet.extended_tweet["full_text"]
            except AttributeError:
                full_text = tweet.text

        try:
            hash_tags = [[tag for key,tag in hash_tag.items() if key == 'text'] for hash_tag in tweet.entities.get('hashtags')]
            hash_tags = [item for sublist in hash_tags for item in sublist]
        except:
            hash_tags = []    

        new_payload = {
            "tweet_id": tweet.id,
            "text": full_text,
            "timestamp": tweet.timestamp_ms,
            "user_id": tweet.user.id,
            "user_verified": tweet.user.verified,
            "user_name": tweet.user.name,
            "hash_tags": hash_tags,
            "followers": tweet.user.followers_count,
            "following": tweet.user.friends_count,
            "tweets_by_user": tweet.user.statuses_count,
            "is_quoting": quoting,
            "is_reply_to": tweet.in_reply_to_status_id,
            "is_retweeting": retweeting
        }

        message.payload = new_payload
        return [message]

class ExtractCVEsOperation(cronicl.operations.Operation):
    
    def find_cves(self, string):
        tokens = re.findall(r"(?i)CVE.\d{4}-\d{4,7}", string) 
        result = []
        for token in tokens:
            token = token.upper().strip()
            token = token[:3] + '-' + token[4:]  # snort rules list cves as CVE,2009-0001
            result.append(token)
        return result

    def execute(self, message):
        payload = message.payload

        cves = self.find_cves(payload.get('text', ''))
        if len(cves) == 0:
            return
        payload['cves'] = cves

        message.payload = payload
        return [message]

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api, pipeline):
        self.api = api
        self.me = api.me()
        self.pipeline = pipeline

    def on_status(self, tweet):
        self.pipeline.execute(tweet)

    def on_error(self, status):
        print("Error detected")

#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(created)f %(message)s")

#cronicl.get_tracer().set_handler(cronicl.StackDriverTracer('cronicl_trace'))


pipeline = nx.DiGraph()
pipeline.add_node('column_extract', function=ExtractTweetDetailsOperation())
pipeline.add_node('cve_extract', function=ExtractCVEsOperation())
pipeline.add_node('print', function=cronicl.operations.ScreenSink())
pipeline.add_node('bigquery', function=cronicl.operations.BigQuerySink(project='vulnerability-analytics',dataset='streaming',table='twitter_cve_table'))

pipeline.add_edge('column_extract', 'cve_extract')
pipeline.add_edge('cve_extract', 'print')
pipeline.add_edge('cve_extract', 'bigquery')

flow = cronicl.Pipeline(pipeline, sample_rate=0.01)
flow.init()
flow.draw()
#flow.execute(None)
#flow.close()

tweets_listener = MyStreamListener(api, flow)
stream = tweepy.Stream(api.auth, tweets_listener, tweet_mode="extended")
stream.filter(track=["CVE"], languages=["en"])