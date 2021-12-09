#!/usr/local/bin/python3
#coding: utf-8
#PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################



print("""
 
 _______  _______  _        ______   _______  _______  _       
(  ____ \(  ___  )( (    /|(  __  \ (       )(  ___  )( (    /|
| (    \/| (   ) ||  \  ( || (  \  )| () () || (   ) ||  \  ( |
| (_____ | (___) ||   \ | || |   ) || || || || (___) ||   \ | |
(_____  )|  ___  || (\ \) || |   | || |(_)| ||  ___  || (\ \) |
      ) || (   ) || | \   || |   ) || |   | || (   ) || | \   |
/\____) || )   ( || )  \  || (__/  )| )   ( || )   ( || )  \  |
\_______)|/     \||/    )_)(______/ |/     \||/     \||/    )_)
                                                               

                                                        
                     Welcome to a dream about book!
- Database of the biggest library that ever existed, but one day it was dreamed of!-                                              

""")
#imports
import json
import csv
from faker import Faker
import faker_commerce
import faker_microservice
from faker_vehicle import VehicleProvider
from faker_music import MusicProvider
import random
from datetime import date, datetime
from create_dataset import Variables
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)
#variables

db = client['sandman']
Collection = db["library"]


val = int(input("INSERT THE AMOUNT OF DATA TO BE GENERATED: "))
Variables(val)
num = 0
for i in range(val):
    num = num + 1
    with open(f'C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/dataset/json_files/book/tables_{num}.json') as file:
         book = json.load(file)
         Collection.insert_one(book)
       

