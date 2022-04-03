#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re
import os


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()


# In[3]:


w_m_game_df = spark.read.parquet("../parquet_files/w_m_game.parquet").toPandas()


# In[4]:


w_m_game_df


# In[5]:


athlete_info = w_m_game_df[['country', 'name','helmet_number']].groupby(['country','name']).nunique().reset_index().drop('helmet_number', axis = 1)


# In[6]:


athlete_info


# In[7]:


athlete_info.name = [i.lower() for i in athlete_info.name]


# In[8]:


athlete_info.country = athlete_info.country.replace(to_replace = 'ROC', value = 'RUS')


# In[9]:


def get_id (df, cols):
    dict_id = {}
    for c, n in zip(df[cols[0]], df[cols[1]]):
        URL =f"http://www.shorttrackonline.info/athletes.php?country={c}"
        page = requests.get(URL)

        soup = BeautifulSoup(page.content, "html.parser")
        soup_body = str(soup.body)
        temp_dict_id_name = {''.join(k.lower().split(',')):v for k,v in zip (re.findall(r'">(.*)</a></td>\n', soup_body), re.findall(r'<a href="skaterbio.php\?id=(.*)">', soup_body))}
    #     print(temp_dict_id_name.keys())
    #     print(n in temp_dict_id_name)
        if n in temp_dict_id_name.keys():
            dict_id[n] = temp_dict_id_name[n]
    return dict_id


# In[10]:


dict_id = get_id(athlete_info, ['country', 'name'])


# In[11]:


athlete_info = athlete_info.merge(pd.DataFrame({'name':dict_id.keys(), 'id': dict_id.values()}), how = 'left', on = 'name', copy = False)


# In[12]:


athlete_info


# In[13]:


athlete_info[athlete_info.id.isnull()]


# In[14]:


athlete_info.name = athlete_info.name.replace('blais danae', 'blais danaé')
athlete_info.name = athlete_info.name.replace('han yutong', 'han yu tong')
athlete_info.name = athlete_info.name.replace('lepape sebastien', 'lepape sébastien')
athlete_info.name = athlete_info.name.replace('lee juneseo', 'lee june seo')
athlete_info.name = athlete_info.name.replace('park janghyuk', 'park jang hyuk')
athlete_info.name = athlete_info.name.replace('airapetian denis', 'ayrapetyan denis')


# In[15]:


athlete_info[athlete_info.id.isnull()]


# In[16]:


dict_id_second = get_id(athlete_info[athlete_info.id.isnull()], ['country', 'name'])


# In[17]:


temp_df = pd.DataFrame({'name':dict_id_second.keys(), 'id': dict_id_second.values()})


# In[18]:


temp_df


# In[19]:


athlete_info.id.fillna(athlete_info['name'].map(dict_id_second), inplace=True)


# In[20]:


athlete_info


# In[21]:


athlete_info['birth_year'] = athlete_info['id'].apply(lambda x: x[-6:-2])


# In[22]:


athlete_info['birth_year'] = athlete_info['birth_year'].astype(int)


# In[23]:


athlete_info['age'] = 2022 - athlete_info['birth_year']


# In[24]:


athlete_info['gender'] = athlete_info['id'].apply(lambda x: x[-11:-10])


# In[25]:


athlete_info.gender = athlete_info.gender.replace({'1': 'Male', '2': 'Female'})


# In[26]:


dict_info = {}
for i in athlete_info['id']:
    URL =f"https://www.shorttrackonline.info/skaterbio.php?id={i}"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    soup_body = str(soup.body)
    age_cate = re.findall(r'Age Category:</td>\n<td class="bio">(.*)</td>', soup_body)
    club = re.findall(r'Club:</td>\n<td class="bio">(.*)</td>', soup_body)
    temp = []
    if len(age_cate) != 0:
        temp.append(age_cate[0])
    else:
        temp.append('')
    if len(club) != 0:
        temp.append(club[0])
    else:
        temp.append('')
    
    dict_info[i] = temp


# In[27]:


athlete_info = athlete_info.merge(pd.DataFrame({'id': dict_info.keys(), 'age_category': [i[0] for i in dict_info.values()], 'club': [i[1] for i in dict_info.values()]}), how = 'left', on = 'id')


# In[28]:


athlete_info


# In[29]:


athlete_info_df = spark.createDataFrame(athlete_info)


# In[30]:


athlete_info_df.printSchema()


# In[31]:


athlete_info_df.write.parquet("parquet_files/athlete_info.parquet", mode = 'overwrite')

