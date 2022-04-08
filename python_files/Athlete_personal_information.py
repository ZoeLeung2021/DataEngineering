#!/usr/bin/env python
# coding: utf-8


# import the packages
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re
import os

from dvc.api import make_checkpoint


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"



# import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()


# read the game results from the parquet file
w_m_game_df = spark.read.parquet("/project/DataEngineering/parquet_files/w_m_game.parquet").toPandas()


# get all the unique athlete information
athlete_info = w_m_game_df[['country', 'name','helmet_number']].groupby(['country','name']).nunique().reset_index().drop('helmet_number', axis = 1)



# set all the athlete name to lower case
athlete_info.name = [i.lower() for i in athlete_info.name]


# replace country code of ROC to RUS
athlete_info.country = athlete_info.country.replace(to_replace = 'ROC', value = 'RUS')




def get_id (df, cols):
    '''
    function to get each athlete id
    '''
    dict_id = {}
    for c, n in zip(df[cols[0]], df[cols[1]]):
        URL =f"http://www.shorttrackonline.info/athletes.php?country={c}"
        page = requests.get(URL)

        soup = BeautifulSoup(page.content, "html.parser")
        soup_body = str(soup.body)
        temp_dict_id_name = {''.join(k.lower().split(',')):v for k,v in zip (re.findall(r'">(.*)</a></td>\n', soup_body), re.findall(r'<a href="skaterbio.php\?id=(.*)">', soup_body))}
        if n in temp_dict_id_name.keys():
            dict_id[n] = temp_dict_id_name[n]
    return dict_id



dict_id = get_id(athlete_info, ['country', 'name'])



# merge the id information with the athlete data frame
athlete_info = athlete_info.merge(pd.DataFrame({'name':dict_id.keys(), 'id': dict_id.values()}), how = 'left', on = 'name', copy = False)
make_checkpoint()


# check the NaN rows
athlete_info[athlete_info.id.isnull()]


# manually replace the names
athlete_info.name = athlete_info.name.replace('blais danae', 'blais danaé')
athlete_info.name = athlete_info.name.replace('han yutong', 'han yu tong')
athlete_info.name = athlete_info.name.replace('lepape sebastien', 'lepape sébastien')
athlete_info.name = athlete_info.name.replace('lee juneseo', 'lee june seo')
athlete_info.name = athlete_info.name.replace('park janghyuk', 'park jang hyuk')
athlete_info.name = athlete_info.name.replace('airapetian denis', 'ayrapetyan denis')


athlete_info[athlete_info.id.isnull()]


# call the function again to get the rest of the athlete id
dict_id_second = get_id(athlete_info[athlete_info.id.isnull()], ['country', 'name'])

# get the new data frame for the NaN athlete
temp_df = pd.DataFrame({'name':dict_id_second.keys(), 'id': dict_id_second.values()})



# map the id and name into the athlete_info data frame
athlete_info.id.fillna(athlete_info['name'].map(dict_id_second), inplace=True)
make_checkpoint()

# get the birth year from each id
athlete_info['birth_year'] = athlete_info['id'].apply(lambda x: x[-6:-2])


athlete_info['birth_year'] = athlete_info['birth_year'].astype(int)
make_checkpoint()


# get the age by 2022-birth_year
athlete_info['age'] = 2022 - athlete_info['birth_year']
make_checkpoint()

# get the gender information from the id
athlete_info['gender'] = athlete_info['id'].apply(lambda x: x[-11:-10])



# replace 1 and 2 with male and female
athlete_info.gender = athlete_info.gender.replace({'1': 'Male', '2': 'Female'})
make_checkpoint()


# get the age category and club information from each athlete
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


# merge the information of age_category and club into the athlete_info data frame
athlete_info = athlete_info.merge(pd.DataFrame({'id': dict_info.keys(), 'age_category': [i[0] for i in dict_info.values()], 'club': [i[1] for i in dict_info.values()]}), how = 'left', on = 'id')
make_checkpoint()

# convert the athlete_info into spark data frame
athlete_info_df = spark.createDataFrame(athlete_info)




athlete_info_df.printSchema()


# convert the data frame into parquet format
athlete_info_df.write.parquet("/project/DataEngineering/parquet_files/athlete_info.parquet", mode = 'overwrite')
make_checkpoint()

