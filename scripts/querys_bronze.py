#./bin/pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.myCollection?readPreference=primaryPreferred" \
#              --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.myCollection" \
#              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1
#coding: utf-8

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Sandman
#     Repositorio: Stagin Bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)





spark = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
df = spark.read.format("mongo").option("database", "sandman").option("collection", "library").load()

#transform
df.createOrReplaceTempView('df')


table_volumeInfo = spark.sql("""SELECT monotonically_increasing_id() as _id,
volumeInfo.title as title,
volumeInfo.Subtitle as subtitle,
volumeInfo.author as author,
date_format(current_date(),'yyyyMMdd') as dreaming_date
FROM df""")

table_scope = spark.sql("""
SELECT monotonically_increasing_id() as _id,
kind,publisher, publishedDate, edition, sample,pageCount, wordCount, capCount, categories,
date_format(current_date(),'yyyyMMdd') as dreaming_date
FROM df""")

table_industry = spark.sql("""
SELECT monotonically_increasing_id() as _id,
industryIdentifiers.type as type,
industryIdentifiers.identifier as identifier,
date_format(current_date(),'yyyyMMdd') as dreaming_date
FROM df
""")

table_saleInfo = spark.sql("""
SELECT monotonically_increasing_id() as _id,
saleInfo.original_price as original_price,
saleInfo.current_prefix as current_prefix,
saleInfo.current_sufix as current_sufix,
saleInfo.barcode as barcode,
date_format(current_date(),'yyyyMMdd') as dreaming_date
FROM df
""")

#####################LOAD###########################################

table_volumeInfo.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/volumeInfo/")
table_volumeInfo.write.mode("overwrite").format("orc").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/orc/volumeInfo/")

table_scope.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/scope/")
table_scope.write.mode("overwrite").format("orc").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/orc/scope/")

table_industry.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/industry/")
table_industry.write.mode("overwrite").format("orc").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/orc/industry/")

table_saleInfo.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/saleInfo/")
table_saleInfo.write.mode("overwrite").format("orc").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/orc/saleInfo /")


spark.stop()

exit(0)
