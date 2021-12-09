#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: SANDMAN
#     Repositorio: stagin gold
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

idi = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/industry").createOrReplaceTempView("idi")
sale = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/saleInfo").createOrReplaceTempView("sale")
scope = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/scope/").createOrReplaceTempView("scope")
volume = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/silver/volumeInfo/").createOrReplaceTempView("volume")

#################################TRANSFORM#################################

sandman = spark.sql("""
SELECT SC._id, SC.kind, 
VL.title, VL.subtitle, VL.author,
SC.publisher,SC.publishedDate,SC.edition, SC.sample,
ID.type, ID.identifier,
SC.pageCount, SC.wordCount, SC.capCount, SC.categories,
SL.original_price, SL.current_prefix, SL.current_sufix, SL.barcode,
SC.dreaming_date
FROM scope SC INNER JOIN volume VL ON SC._id = VL._id
INNER JOIN sale SL ON SC._id = SL._id
INNER JOIN idi ID ON SC._id = ID._id

""")

#################################LOAD#################################

sandman.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/gold/sandman")
