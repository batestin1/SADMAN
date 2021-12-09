#!/usr/local/bin/python3
#coding: utf-8
#VARIABLES

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Sandman
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
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


#setup
faker = Faker()


class Variables():
    def __init__(self, val) -> None:
        num = 0
        for i in range(val):
            num = num + 1
            with open(f'C:/Users/Bates/Documents/Repositorios/NOSQL/sandman/dataset/json_files/book/tables_{num}.json', "w") as output:
                one_word = faker.word()
                two_word = f"{faker.word()} {faker.word()}"
                tree_word = f"{faker.word()} {faker.word()} {faker.word()}"
                words = faker.sentence()
                title = random.choice([one_word,two_word,tree_word, words]).upper()
                subtitle = random.choice([faker.paragraph(nb_sentences=5), "Don't Have"]).upper()

                author = faker.name().upper()
                authors = [faker.name().upper(), faker.name().upper()]

                ext = faker.file_extension(category='text')
                list_kind = [
                    'books#volume',
                    f'{ext}#volume']
                kind = random.choice(list_kind)
                ext_group = random.choice([
                    "group",
                    "associates",
                    "independent",
                    "institute"
                ])
                ext_word = f"{faker.word()} {ext_group}"
                ext_word2 = f"{faker.word()} {faker.word()} {ext_group} "
                ext_word3 = f"{faker.word()} {faker.word()} {faker.word()} {ext_group}"
                publisher = random.choice([ext_word, ext_word2, ext_word3]).upper()
                publishedDate  = faker.year()
                sample = f"""...{faker.text().lower()}."""
                typeIsbnTotal = random.choice([{"ISBN_10": faker.isbn10()}, {"ISBN_13": faker.isbn13()}])
                typeIsbnTotal2 = random.choice([{"ISBN_10": faker.isbn10()}, {"ISBN_13": faker.isbn13()}])
                typeIsbn2 = list(typeIsbnTotal2.keys())[0]
                isbn2 = list(typeIsbnTotal2.values())[0]
                edition = f"{random.randint(1,9)}º EDITION"
                typeIsbn = list(typeIsbnTotal.keys())[0]
                isbn = list(typeIsbnTotal.values())[0]
                pageCount = random.randint(1,1000)
                wordCount = random.randint(1000,100000)
                capCount = random.randint(1,20)
                text = random.choice(["false","true"])
                categories = random.choice([
                    'Biographical Novel',
                    'Epistolary Novel',
                    'Historical novel',
                    'Psychological Novel',
                    'Dramatic comedy',
                    'Novel',
                    'Tale',
                    'Chronicle',
                    'Rehearsal',
                    'Poetry',
                    'Letter',
                    'NON-FICTION',
                    'Biography',
                    'Memoirs',
                    'Adventure',
                    'Chick-Lit',
                    'Graphic Novel',
                    'Fantastic literature',
                    'Children s literature',
                    'Children s Literature',
                    'Magical Realism',
                    'Horror',
                    'Thriller',
                    'Comedy',
                    'Western',
                    'Policeman',
                    'Investigation',
                    'Academic article',
                    'Scientific article',
                    'Monography',
                    'Completion of course work',
                    'Masters dissertation',
                    'Doctoral thesis']).upper()
                price = float(random.randint(1,100))
                barcode = faker.ean13()
                current = faker.currency()
                currentPrefix = current[0]
                currentSufix = current[1]
                plan = random.randint(1,2)

                if plan == 1:

                    book = {
                        "kind": kind,
                        "volumeInfo": {
                            "title": title,
                            "Subtitle": subtitle,
                            "author": author
                        },
                        "publisher": publisher,
                        "publishedDate": publishedDate,
                        "edition": edition,
                        "sample": sample,
                        "industryIdentifiers": [
                            {"type": typeIsbn,
                            "identifier": isbn
                            },
                            {"type": typeIsbn2,
                            "identifier": isbn2
                            }],
                        "pageCount": pageCount,
                        "wordCount": wordCount,
                        "capCount": capCount,
                        "categories":[categories],
                        "saleInfo": {
                            "original_price":  price,
                            "current_prefix": currentPrefix,
                            "current_sufix": currentSufix,
                            "barcode": barcode
                        },
                        }
                          
                    json.dump(book, output, allow_nan=True, indent=True, separators=(',',':'))
                
                else:

                    book = {
                    "kind": kind,
                    "volumeInfo": {
                        "title": title,
                        "Subtitle": subtitle,
                        "author": authors
                    },
                    "publisher": publisher,
                    "publishedDate": publishedDate,
                    "edition": edition,
                    "sample": sample,
                    "industryIdentifiers": [
                        {"type": typeIsbn,
                        "identifier": isbn
                        },
                        {"type": typeIsbn2,
                        "identifier": isbn2
                        }],
                    "pageCount": pageCount,
                    "wordCount": wordCount,
                    "capCount": capCount,
                    "categories":[categories],
                    "saleInfo": {
                        "original_price":  price,
                        "current_prefix": currentPrefix,
                        "current_sufix": currentSufix,
                        "barcode": barcode
                    },
                    }
                        
                    json.dump(book, output, allow_nan=True, indent=True, separators=(',',':'))
                    
