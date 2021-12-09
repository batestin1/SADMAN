#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: SANDMAN
#     Repositorio: stagin silver
# 
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

id = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/industry/").createOrReplaceTempView("id")
sale = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/saleInfo").createOrReplaceTempView("sale")
scop = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/scope/").createOrReplaceTempView("scope")
volume = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/bronze/parquet/volumeInfo/").createOrReplaceTempView("volume")

#################################TRANSFORM#################################

typeA = spark.sql("""SELECT _id, explode(type) as type, identifier, dreaming_date FROM id""").createOrReplaceTempView("typeA")

typeB = spark.sql("""SELECT monotonically_increasing_id() as _id, type, explode(identifier) as identifier , dreaming_date FROM typeA""").createOrReplaceTempView("typeB")

industry = spark.sql(""" SELECT l. 
* FROM (SELECT _id, type, identifier,dreaming_date, row_number() over 
(partition by _id order by dreaming_date desc) as row_id FROM typeB WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

sale = spark.sql("""
SELECT l. * FROM(SELECT _id, original_price, current_prefix, current_sufix, barcode, dreaming_date,
row_number() over (partition by _id order by dreaming_date desc) as row_id FROM sale  WHERE TRIM(_id) <> '') l WHERE row_id = 1
"""
)

scop = spark.sql("""
SELECT l. * FROM(SELECT _id, kind, publisher, publishedDate, edition, sample, pageCount, wordCount, capCount,
explode(categories) as categories, dreaming_date,
row_number() over (partition by _id order by dreaming_date desc) as row_id FROM scope  WHERE TRIM(_id) <> '') l WHERE row_id = 1

""")

volume = spark.sql("""
SELECT l. * FROM(SELECT _id, title, subtitle, author, dreaming_date, row_number() over(partition by _id order by dreaming_date desc) as row_id FROM volume WHERE TRIM(_id) <> '') l WHERE row_id = 1
""")


industry.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/industry")
sale.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/saleInfo")
scop.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/scope")
volume.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/volumeInfo")

