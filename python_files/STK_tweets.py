#!/usr/bin/env python
# coding: utf-8

# In[4]:


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


# In[5]:


stk_tweet = pd.DataFrame({'id':[tweet['id'] for tweet in tweets['data']], 'text':[tweet['text'] for tweet in tweets['data']]})


# In[6]:


stk_tweet


# In[25]:


get_ipython().system('pip install vaderSentiment')


# In[ ]:


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer 


# In[ ]:


analyzer = SentimentIntensityAnalyzer()
list_all_score = []
for t in stk_tweet['text']:
    vs = analyzer.polarity_scores(t)
    print(t)
    print(vs)
    list_all_score.append(vs)


# In[ ]:


stk_tweet = pd.concat([stk_tweet, pd.DataFrame({'neg':[i['neg'] for i in list_all_score],
             'neu':[i['neu'] for i in list_all_score],
             'pos':[i['pos'] for i in list_all_score],
             'compound':[i['compound'] for i in list_all_score]})], axis = 1)


# In[ ]:


stk_tweet


# In[ ]:


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"


# In[ ]:


from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()


# In[ ]:


stk_tweet_df = spark.createDataFrame(stk_tweet)


# In[ ]:


stk_tweet_df.printSchema()


# In[ ]:


stk_tweet_df.write.parquet("parquet_files/stk_tweet.parquet", mode = 'overwrite')

