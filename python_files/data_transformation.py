#!/usr/bin/env python
# coding: utf-8


# import packages
import os
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from dvc.api import make_checkpoint



os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"



from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()



# read the parquet files
w_m_game_df = spark.read.parquet("/project/DataEngineering/parquet_files/w_m_game.parquet").toPandas()
relay_df =  spark.read.parquet("/project/DataEngineering/parquet_files/relay.parquet").toPandas()
athlete_info_df = spark.read.parquet("/project/DataEngineering/parquet_files/athlete_info.parquet").toPandas()



# replace all the ROC with RUS
w_m_game_df.country = w_m_game_df.country.replace(to_replace = 'ROC', value = 'RUS')
relay_df.country = relay_df.country.replace(to_replace = 'ROC', value = 'RUS')
relay_df.name = relay_df.name.replace(to_replace = 'ROC', value = 'RUS')
make_checkpoint()




# set the athlete name into lower cases
w_m_game_df ['name'] = w_m_game_df['name'].apply(lambda x: x.lower())
make_checkpoint()




# replace the special cases
w_m_game_df.name = w_m_game_df.name.replace('blais danae', 'blais danaé')
w_m_game_df.name = w_m_game_df.name.replace('han yutong', 'han yu tong')
w_m_game_df.name = w_m_game_df.name.replace('lepape sebastien', 'lepape sébastien')
w_m_game_df.name = w_m_game_df.name.replace('lee juneseo', 'lee june seo')
w_m_game_df.name = w_m_game_df.name.replace('park janghyuk', 'park jang hyuk')
w_m_game_df.name = w_m_game_df.name.replace('airapetian denis', 'ayrapetyan denis')
make_checkpoint()



# check the special cases in qualified column
w_m_game_df[w_m_game_df['qualified'].apply(lambda x: len(x)>1)]['qualified']




# merge the athlete information with the women and men game information
w_m_game_df = w_m_game_df.merge(athlete_info_df[['name', 'id']], how = 'left', on = 'name')
make_checkpoint()




def get_qualified(x):
    '''
    only retrun the qualified details
    '''
    if len(x) == 1 and x[0] != 'OR':
        return x[0]
    elif len(x) == 2:
        return x[1]
    else:
        return ''




# apply the function on both women & men game information and relay game information
w_m_game_df.qualified = w_m_game_df['qualified'].apply(get_qualified)
make_checkpoint()
relay_df.qualified = relay_df['qualified'].apply(get_qualified)
make_checkpoint()



# check the information returned
set(w_m_game_df.qualified)



# check the information returned
set(relay_df.qualified)



def get_special_cases(df):
    '''
    convert the special cases in time column into the qualified column
    '''
    index = df[df['time'].isin([ 'PEN', 'No Time', 'DNS', 'YC','DNF']) & (df['qualified']=='')].index
    df['qualified'].loc[index] = df['time'].loc[index]
    return df



# apply to both data frame
w_m_game_df = get_special_cases(w_m_game_df)
make_checkpoint()
relay_df = get_special_cases(relay_df)
make_checkpoint()




# check there are only two types of time format in time column
w_m_game_df[~w_m_game_df['time'].apply(lambda x: ':' in x or '.' in x)]




# check there are only two types of time format in time column
relay_df[~relay_df['time'].apply(lambda x: ':' in x or '.' in x)]




def timestamp(x):
    '''
    convert the string of time into timestamp format
    '''
    if ':' in x:
        return datetime.strptime(x,'%M:%S.%f').timestamp()
    elif  '.' in x:
        return datetime.strptime(x, '%S.%f').timestamp()
    else:
        return 0




# apply the function
w_m_game_df['timestamp'] = w_m_game_df['time'].apply(timestamp)
make_checkpoint()
relay_df['timestamp'] = relay_df['time'].apply(timestamp)
make_checkpoint()




# split the game column into two columns game_type and level
w_m_game_df[['game_type', 'level']] = w_m_game_df.game.str.split(expand = True)
make_checkpoint()
relay_df[['game_type', 'level']] = relay_df.game.str.split(expand = True)
make_checkpoint()



# create a new column and filled with 0
w_m_game_df['rank_by_game'] = 0
relay_df['rank_by_game'] = 0



def rank_game(df):
    '''
    function to get the overall rank for each game and event
    '''
    temp_other = df[df['level']!='FNL']
    temp_other['rank_by_game'] = temp_other.groupby(['level', 'game_type'])['timestamp'].rank(method='min', ascending = True).astype(np.int64)
    
    temp_fnl = df[df['level'] == 'FNL']
    list_game = set(temp_fnl['game_type'])
    for g in list_game:
        index_A = temp_fnl[(temp_fnl['game_type'] == g) & (temp_fnl['group'].apply(lambda x: 'A' in x))].index
        index_B = temp_fnl[(temp_fnl['game_type'] == g) & (temp_fnl['group'].apply(lambda x: 'B' in x))].sort_values('rank').index
        max_rank_groupA = int(temp_fnl[(temp_fnl['game_type'] == g) & (temp_fnl['group'].apply(lambda x: 'A' in x))]['rank'].max())
        rank_B = [i+max_rank_groupA for i in range(1,len(index_B)+1)]
        
        temp_fnl['rank_by_game'].loc[index_A] = temp_fnl['rank'].loc[index_A]
        temp_fnl['rank_by_game'].loc[index_B] = rank_B
        
    return temp_other.append(temp_fnl)


# apply the function
w_m_game_df = rank_game(w_m_game_df)
make_checkpoint()
relay_df = rank_game(relay_df)
make_checkpoint()



# convert the type into integer
w_m_game_df.rank_by_game = w_m_game_df.rank_by_game.astype(np.int64)
make_checkpoint()
relay_df.rank_by_game = relay_df.rank_by_game.astype(np.int64)
make_checkpoint()


# example of one of the game
w_m_game_df[(w_m_game_df['game_type'] == 'M1000M') & (w_m_game_df['level'] == 'QFNL')].sort_values('rank_by_game')




# get all the country information
URL =f"http://www.shorttrackonline.info/athletes.php?"
page = requests.get(URL)

soup = BeautifulSoup(page.content, "html.parser")
soup_body = str(soup.body)
dict_country = {i.split('">')[0]:i.split('">')[1] for i in re.findall(r'country=(.*)</a', soup_body)}


# convert the dictionary into data frame
countries = pd.DataFrame({'country_code':dict_country.keys(), 'name': dict_country.values()})
make_checkpoint()



# based on the schema get all the tables
heat = pd.DataFrame(w_m_game_df[w_m_game_df['level'] == 'HEAT'][['id', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game', 'qualified']].reset_index(drop = True))
qfnl = pd.DataFrame(w_m_game_df[w_m_game_df['level'] == 'QFNL'][['id', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game', 'qualified']].reset_index(drop = True))
sfnl = pd.DataFrame(w_m_game_df[w_m_game_df['level'] == 'SFNL'][['id', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game', 'qualified']].reset_index(drop = True))
fnl = pd.DataFrame(w_m_game_df[w_m_game_df['level'] == 'FNL'][['id', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game']].reset_index(drop = True))
make_checkpoint()


# change the columns name
heat.columns = ['id', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank', 'qualified']
qfnl.columns = ['id', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank', 'qualified']
sfnl.columns = ['id', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank', 'qualified']
fnl.columns = ['id', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank']
make_checkpoint()




# based on the schema and get all the tables
relay_qfnl = pd.DataFrame(relay_df[relay_df['level'] == 'QFNL'][['country', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game', 'qualified']].reset_index(drop = True))
relay_sfnl = pd.DataFrame(relay_df[relay_df['level'] == 'SFNL'][['country', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game', 'qualified']].reset_index(drop = True))
relay_fnl = pd.DataFrame(relay_df[relay_df['level'] == 'FNL'][['country', 'game_type', 'group', 'timestamp', 'rank', 'rank_by_game']].reset_index(drop = True))
make_checkpoint()


# change the columns name
relay_qfnl.columns = ['country_code', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank', 'qualified']
relay_sfnl.columns = ['country_code', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank', 'qualified']
relay_fnl.columns = ['country_code', 'game_type', 'group', 'time', 'inGroup_rank', 'game_rank']
make_checkpoint()




# get the information needed for athlete information
athlete = pd.DataFrame(athlete_info_df[['id','name', 'country', 'birth_year', 'age_category', 'club']])
make_checkpoint()





# change the columns
athlete.columns = ['id', 'name', 'country_code', 'birth_year', 'age_category', 'club']
make_checkpoint()



# change the data type into int type
athlete.birth_year = athlete.birth_year.astype(np.int64)



# convert all the data frame into spark data frame
heat_df = spark.createDataFrame(heat)
qfnl_df = spark.createDataFrame(qfnl)
sfnl_df = spark.createDataFrame(sfnl)
fnl_df = spark.createDataFrame(fnl)
relay_qfnl_df = spark.createDataFrame(relay_qfnl)
relay_sfnl_df = spark.createDataFrame(relay_sfnl)
relay_fnl_df = spark.createDataFrame(relay_fnl)
countries_df = spark.createDataFrame(countries)
athlete_df = spark.createDataFrame(athlete)
make_checkpoint()




heat_df.printSchema()
qfnl_df.printSchema()
sfnl_df.printSchema()
fnl_df.printSchema()
relay_qfnl_df.printSchema()
relay_sfnl_df.printSchema()
relay_fnl_df.printSchema()
countries_df.printSchema()
athlete_df.printSchema()





def change_data_type(df, col, type_):
    '''
    function to change the data type for spark data frame
    '''
    return df.withColumn(col,  df[col].cast(type_))





athlete_df = change_data_type(athlete_df, 'birth_year', 'int')
heat_df = change_data_type(heat_df, 'time', 'timestamp')
heat_df = change_data_type(heat_df, 'inGroup_rank', 'int')
heat_df = change_data_type(heat_df, 'game_rank', 'int')
make_checkpoint()

qfnl_df = change_data_type(qfnl_df, 'time', 'timestamp')
qfnl_df = change_data_type(qfnl_df, 'inGroup_rank', 'int')
qfnl_df = change_data_type(qfnl_df, 'game_rank', 'int')
make_checkpoint()

sfnl_df = change_data_type(sfnl_df, 'time', 'timestamp')
sfnl_df = change_data_type(sfnl_df, 'inGroup_rank', 'int')
sfnl_df = change_data_type(sfnl_df, 'game_rank', 'int')
make_checkpoint()

fnl_df = change_data_type(fnl_df, 'time', 'timestamp')
fnl_df = change_data_type(fnl_df, 'inGroup_rank', 'int')
fnl_df = change_data_type(fnl_df, 'game_rank', 'int')
make_checkpoint()

relay_qfnl_df = change_data_type(relay_qfnl_df, 'time', 'timestamp')
relay_qfnl_df = change_data_type(relay_qfnl_df, 'inGroup_rank', 'int')
relay_qfnl_df = change_data_type(relay_qfnl_df, 'game_rank', 'int')
make_checkpoint()

relay_sfnl_df = change_data_type(relay_sfnl_df, 'time', 'timestamp')
relay_sfnl_df = change_data_type(relay_sfnl_df, 'inGroup_rank', 'int')
relay_sfnl_df = change_data_type(relay_sfnl_df, 'game_rank', 'int')
make_checkpoint()

relay_fnl_df = change_data_type(relay_fnl_df, 'time', 'timestamp')
relay_fnl_df = change_data_type(relay_fnl_df, 'inGroup_rank', 'int')
relay_fnl_df = change_data_type(relay_fnl_df, 'game_rank', 'int')
make_checkpoint()



heat_df.printSchema()
qfnl_df.printSchema()
sfnl_df.printSchema()
fnl_df.printSchema()
relay_qfnl_df.printSchema()
relay_sfnl_df.printSchema()
relay_fnl_df.printSchema()
countries_df.printSchema()
athlete_df.printSchema()


# convert all data frames into parquet files
heat_df.write.parquet("/project/DataEngineering/parquet_files/heat.parquet", mode = 'overwrite')
qfnl_df.write.parquet("/project/DataEngineering/parquet_files/qfnl.parquet", mode = 'overwrite')
sfnl_df.write.parquet("/project/DataEngineering/parquet_files/sfnl.parquet", mode = 'overwrite')
fnl_df.write.parquet("/project/DataEngineering/parquet_files/fnl.parquet", mode = 'overwrite')
relay_qfnl_df.write.parquet("/project/DataEngineering/parquet_files/relay_qfnl.parquet", mode = 'overwrite')
relay_sfnl_df.write.parquet("/project/DataEngineering/parquet_files/relay_sfnl.parquet", mode = 'overwrite')
relay_fnl_df.write.parquet("/project/DataEngineering/parquet_files/relay_fnl.parquet", mode = 'overwrite')
countries_df.write.parquet("/project/DataEngineering/parquet_files/countries.parquet", mode = 'overwrite')
athlete_df.write.parquet("/project/DataEngineering/parquet_files/athlete.parquet", mode = 'overwrite')
make_checkpoint()
