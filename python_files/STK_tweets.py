#!/usr/bin/env python
# coding: utf-8


from dvc.api import make_checkpoint


# get the tweets using the twitter api
import pandas as pd
import requests
import json

api_key = 'wkxIkajRgLTVkbL2N9zz0G7RN'
api_secret_key = 'nXuL6pk0OaluzSzxIjIbSBhbsz5EfBrsULX6IatiPJ3D00Auk5'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAADx2aQEAAAAAfdKUH18l6uqR9DAwzr4fER9CS1U%3DnHcc0nZYYe8JfMdYMSnlkDiE1Qiupp6zVqRxzUQpoTNn9e78V5'


query = "Short Track Skating"

# Prepare the headers to pass the authentication to Twitter's api
headers = {
    'Authorization': 'Bearer {}'.format(bearer_token),
}

params = (
    ('query', query),
)

# Does the request to get the most recent tweets
response = requests.get('https://api.twitter.com/2/tweets/search/recent', headers=headers, params=params)

# Validates that the query was successful
if response.status_code == 200:
    print("URL of query:", response.url)
    
    # Let's convert the query result to a dictionary that we can iterate over
    tweets =  json.loads(response.text)
    
    for tweet in tweets['data']:
        print("tweet_id: ", tweet['id'], "tweet_text: ", tweet['text'])



# convert the tweets information into data frame
stk_tweet = pd.DataFrame({'id':[tweet['id'] for tweet in tweets['data']], 'text':[tweet['text'] for tweet in tweets['data']]})
make_checkpoint()





from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer 


# doing the sentiment analysis
analyzer = SentimentIntensityAnalyzer()
list_all_score = []
for t in stk_tweet['text']:
    vs = analyzer.polarity_scores(t)
    print(t)
    print(vs)
    list_all_score.append(vs)



# merge the sentiment data for each row
stk_tweet = pd.concat([stk_tweet, pd.DataFrame({'neg':[i['neg'] for i in list_all_score],
             'neu':[i['neu'] for i in list_all_score],
             'pos':[i['pos'] for i in list_all_score],
             'compound':[i['compound'] for i in list_all_score]})], axis = 1)
make_checkpoint()




import os



os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"




from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()




# convert the data frame into spark data frame
stk_tweet_df = spark.createDataFrame(stk_tweet)




stk_tweet_df.printSchema()




# convert the data frame into parquet format
stk_tweet_df.write.parquet("/project/DataEngineering/parquet_files/stk_tweet.parquet", mode = 'overwrite')
make_checkpoint()