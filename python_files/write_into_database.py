#!/usr/bin/env python
# coding: utf-8



from dvc.api import make_checkpoint
import os



from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()



# create the schema
os.system("PGPASSWORD=qwerty123 psql -h depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com -d haiyunzou21 -U haiyunzou21 -c '\\i shorttrack.sql'")




# read all the tables
heat = spark.read.parquet("/project/DataEngineering/parquet_files/heat.parquet")
qfnl = spark.read.parquet("/project/DataEngineering/parquet_files/qfnl.parquet")
sfnl = spark.read.parquet("/project/DataEngineering/parquet_files/sfnl.parquet")
fnl = spark.read.parquet("/project/DataEngineering/parquet_files/fnl.parquet")
relay_qfnl = spark.read.parquet("/project/DataEngineering/parquet_files/relay_qfnl.parquet")
relay_sfnl = spark.read.parquet("/project/DataEngineering/parquet_files/relay_sfnl.parquet")
relay_fnl = spark.read.parquet("/project/DataEngineering/parquet_files/relay_fnl.parquet")
countries = spark.read.parquet("/project/DataEngineering/parquet_files/countries.parquet")
athlete = spark.read.parquet("/project/DataEngineering/parquet_files/athlete.parquet")
make_checkpoint()




# information for log into postgresql
postgres_uri = "jdbc:postgresql://depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com:5432/haiyunzou21"
user = "haiyunzou21"
password = "qwerty123"


# write each table into the database
countries.write.jdbc(url=postgres_uri, table="shorttrack.countries", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
athlete.write.jdbc(url=postgres_uri, table="shorttrack.athlete", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
heat.write.jdbc(url=postgres_uri, table="shorttrack.heat", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
qfnl.write.jdbc(url=postgres_uri, table="shorttrack.qfnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
sfnl.write.jdbc(url=postgres_uri, table="shorttrack.sfnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
fnl.write.jdbc(url=postgres_uri, table="shorttrack.fnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
relay_qfnl.write.jdbc(url=postgres_uri, table="shorttrack.relay_qfnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
relay_sfnl.write.jdbc(url=postgres_uri, table="shorttrack.relay_sfnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
relay_fnl.write.jdbc(url=postgres_uri, table="shorttrack.relay_fnl", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()

