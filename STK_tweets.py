#!/usr/bin/env python
# coding: utf-8

# In[34]:


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


# In[35]:


stk_tweet = pd.DataFrame({'id':[tweet['id'] for tweet in tweets['data']], 'text':[tweet['text'] for tweet in tweets['data']]})


# In[ ]:




