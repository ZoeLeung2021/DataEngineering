#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os


# In[2]:


def get_file(path):
    with open(path, 'r') as f:
        contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    soup_body = str(soup.body)
    return soup_body


# In[3]:


def women_men_games(event, paths, df):
    soup_body = get_file(paths)
    pattern_country = r'<div class="playerTag" country="(.*)" register='
    pattern_name = r'<td data-sort="(.*)">'
    pattern_time = r'</span></a></div></div></td>\n<td class="text-right">\n(.*)'
    pattern_helmet = r'<td class="text-right d-none d-md-table-cell">\n(.*)</td>'
    pattern_group = r'splitContentResult-STK(.*)" role="row"'
    pattern_rank = r'<td class="text-right sorting_1" data-sort="(.*)">'
    pattern_Q_q = r'<td class="text-right">\n([\s\S]*?)\n</td>\n<td class="text-center d-none d-md-table-cell">'

    country = re.findall(pattern_country,soup_body)
    name = re.findall(pattern_name,soup_body)
    time = re.findall(pattern_time,soup_body)
    helmet = re.findall(pattern_helmet,soup_body)
    group_info =  re.findall(pattern_group,soup_body)
    rank =  re.findall(pattern_rank,soup_body)
    qualified = re.findall(pattern_Q_q,soup_body)

    time = [i.replace('</td>', '').strip() if 'td' in i else 'No Time' if 'No Time' in i else i for i in time]

    helmet = [int(i.strip()) for i in helmet]

    country = country[2:]

    group = [i[i.index(event)+len(event):i.index(event)+len(event)+7] for i in group_info]
    
    game_name = [i[:i.index(event)+len(event)].replace('-', ' ') for i in group_info]

    qualified = [re.findall(r'">(.*)</abbr></strong>',i)  for i in qualified]
    
    
    df['country'] = country
    df['helmet_number'] = helmet
    df['name'] = name
    df['group'] = group
    df['game'] = game_name
    df['rank'] = rank
    df['time'] = time
    df['qualified'] = qualified
    return df


# In[4]:


list_files = list(os.listdir('Html_files'))


# In[5]:


list_files = ['Html_files/' + i for i in list_files]


# In[6]:


df_W500_sfnl = pd.DataFrame()
df_M1000_heat = pd.DataFrame()
df_M500_qfnl = pd.DataFrame()
df_M1000_fnl = pd.DataFrame()
df_M500_heat = pd.DataFrame()
df_M1000_qfnl = pd.DataFrame()
df_W1000_sfnl = pd.DataFrame()
df_M500_fnl = pd.DataFrame()
df_W1000_qfnl = pd.DataFrame()
df_W500_fnl = pd.DataFrame()
df_M500_sfnl = pd.DataFrame()
df_W500_heat = pd.DataFrame()
df_W500_qfnl = pd.DataFrame()
df_M1000_sfnl = pd.DataFrame()
df_W1000_heat = pd.DataFrame()
df_W1000_fnl = pd.DataFrame()
df_M1500_qfnl = pd.DataFrame()
df_W1500_sfnl = pd.DataFrame()
df_M5000R_fnl = pd.DataFrame()
df_MixR_sfnl = pd.DataFrame()
df_W3000R_fnl = pd.DataFrame()
df_M1500_fnl = pd.DataFrame()
df_W1500_fnl = pd.DataFrame()
df_MixR_fnl = pd.DataFrame()
df_W3000R_sfnl = pd.DataFrame()
df_M1500_sfnl = pd.DataFrame()
df_W1500_qfnl = pd.DataFrame()
df_M5000R_sfnl = pd.DataFrame()
df_MixR_qfnl = pd.DataFrame()


# In[7]:


list_df = [
    df_W500_sfnl,
    df_M1000_heat,
    df_M500_qfnl,
    df_M1000_fnl,
    df_M500_heat,
    df_M1000_qfnl,
    df_W1000_sfnl,
    df_M500_fnl,
    df_W1000_qfnl,
    df_W500_fnl,
    df_M500_sfnl,
    df_W500_heat,
    df_W500_qfnl,
    df_M1000_sfnl,
    df_W1000_heat,
    df_W1000_fnl,
    df_M1500_qfnl,
    df_W1500_sfnl,
    df_M1500_fnl,
    df_W1500_fnl,
    df_M1500_sfnl,
    df_W1500_qfnl,
]

list_relay_df = [
    df_M5000R_fnl,
    df_MixR_sfnl,
    df_W3000R_fnl,
    df_MixR_fnl,
    df_W3000R_sfnl,
    df_M5000R_sfnl,
    df_MixR_qfnl
]


# In[8]:


list_event = [
'SFNL',
'HEAT',
'QFNL',
'FNL',
'HEAT',
'QFNL',
'SFNL',
'FNL',
'QFNL',
'FNL',
'SFNL',
'HEAT',
'QFNL',
'SFNL',
'HEAT',
'FNL',
'QFNL',
'SFNL',
'FNL',
'FNL',
'SFNL',
'QFNL',
]

list_relay_event = [
    'FNL',
    'SFNL',
    'FNL',
    'FNL',
    'SFNL',
    'SFNL',
    'QFNL']


# In[9]:


list_files.remove("Html_files/Men's 5000m Relay - Finals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Mixed Team Relay - Semifinals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Women's 3000m Relay - Finals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Mixed Team Relay - Finals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Women's 3000m Relay - Semifinals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Men's 5000m Relay - Semifinals Results - Olympic Short Track Speed Skating.html")
list_files.remove("Html_files/Mixed Team Relay - Quarterfinals Results - Olympic Short Track Speed Skating.html")

list_relay_files = ["Html_files/Men's 5000m Relay - Finals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Mixed Team Relay - Semifinals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Women's 3000m Relay - Finals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Mixed Team Relay - Finals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Women's 3000m Relay - Semifinals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Men's 5000m Relay - Semifinals Results - Olympic Short Track Speed Skating.html",
             "Html_files/Mixed Team Relay - Quarterfinals Results - Olympic Short Track Speed Skating.html"]


# In[10]:


for i,df in enumerate(list_df):
    df = women_men_games(list_event[i], list_files[i],df)


# In[11]:


def relay(event, paths, df):
    soup_body = get_file(paths)
    pattern_country = r'<td class="text-right" data-sort="(.*)">\n<div'
    pattern_name = r'<td data-sort="(.*)">'
    pattern_time = r'</a></div></div></td>\n<td class="text-right">\n(.*)'
    pattern_group = r'splitContentResult-STK(.*)" role="row"'
    pattern_rank = r'<td class="text-right sorting_1" data-sort="(.*)">'
    pattern_Q_q = r'<td class="text-right">\n([\s\S]*?)\n</td>\n<td class="text-center d-none d-md-table-cell">'

    country = re.findall(pattern_country,soup_body)
    name = re.findall(pattern_name,soup_body)
    time = re.findall(pattern_time,soup_body)
    group_info =  re.findall(pattern_group,soup_body)
    rank =  re.findall(pattern_rank,soup_body)
    qualified = re.findall(pattern_Q_q,soup_body)
    
    time = [i.replace('</td>', '').strip() if 'td' in i else 'No Time' if 'No Time' in i else i for i in time]

    group = [i[i.index(event)+len(event):i.index(event)+len(event)+6] for i in group_info]
    
    game_name = [i[:i.index(event)+len(event)].replace('-', ' ') for i in group_info]


    qualified = [re.findall(r'">(.*)</abbr></strong>',i)  for i in qualified]


    df['country'] = country
    df['name'] = name
    df['group'] = group
    df['group_name'] = game_name
    df['rank'] = rank
    df['time'] = time
    df['qualified'] = qualified
    return df


# In[12]:


for i,df in enumerate(list_relay_df):
    df = relay(list_relay_event[i], list_relay_files[i],df)


# In[13]:


def concat(df,list_):
    for i in list_:
        df = df.append(i)
    list_time = []
    for count, time in enumerate(df['time']):
        if '>' in time:
            list_time.append(re.findall(r'>(.*)</abbr>', time)[0])
        else:
            list_time.append(time)
    df['time'] = list_time
    return df


# In[14]:


df_w_m_game = pd.DataFrame()
df_w_m_game = concat(df_w_m_game, list_df)


# In[15]:


df_relay_game = pd.DataFrame()
df_relay_game = concat(df_relay_game, list_relay_df)


# In[16]:


df_w_m_game


# In[17]:


df_relay_game


# In[18]:


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"


# In[19]:


from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()


# In[20]:


w_m_game_spark_df = spark.createDataFrame(df_w_m_game)
relay_spark_df = spark.createDataFrame(df_relay_game)


# In[21]:


w_m_game_spark_df.printSchema()


# In[22]:


relay_spark_df.printSchema()


# In[23]:


w_m_game_spark_df.write.parquet("parquet_files/w_m_game.parquet", mode = 'overwrite')
relay_spark_df.write.parquet("parquet_files/relay.parquet", mode = 'overwrite')

