#!/usr/local/bin/python3
# PERSONA
#export LANG=us_EN

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: SANDMAN
#     Repositorio: output/mysql
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import mysql.connector
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')
#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE sandman')
my_conn = create_engine('mysql+mysqldb://root:@localhost/sandman?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()


#connection

sandman = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/stagin/gold/sandman").createOrReplaceTempView("sandman")


#################################TRANSFORM################################
sand = spark.sql("SELECT * FROM sandman")

resume = spark.sql("""SELECT metric, value FROM(SELECT 1 as index, 'Total of Dreamns' metric, COUNT(distinct author) as value FROM sandman UNION
SELECT 2 as index, 'Total of Books' metric, COUNT(distinct title) as value FROM sandman UNION
SELECT 3 as index, 'Total of Data' metric, COUNT(distinct _id) as value FROM sandman)

""")

#################################LOAD################################


sand.write.mode("overwrite").format("parquet").partitionBy("dreaming_date").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/output/raw/sandman")
resume.write.mode("overwrite").format("parquet").save("C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/output/resume/sandman")
sand.write.format('jdbc').options(url='jdbc:mysql://localhost/sandman?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='library',user='root',password='').mode('append').save()
resume.write.format('jdbc').options(url='jdbc:mysql://localhost/sandman?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_info',user='root',password='').mode('append').save()