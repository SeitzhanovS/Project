#!/bin/bash

import sqlalchemy
import csv
import json


engine = sqlalchemy.create_engine("mysql://codetest:swordfish@localhost/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

People = sqlalchemy.schema.Table('People', metadata, autoload=True, autoload_with=engine)
Places = sqlalchemy.schema.Table('Places', metadata, autoload=True, autoload_with=engine)

print('Starting ingestion to Places table')
connection.execute(Places.delete())
with open('data/places.csv',encoding='utf-8') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader:
    connection.execute(Places.insert().values(city = row[0], county = row[1], country = row[2]))

print('Finished ingestion to Places table')

print('Starting ingestion to People table')
connection.execute(People.delete())
with open('data/people.csv',encoding='utf-8') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader:
    connection.execute(People.insert().values(given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth = row[3]))

print('Finished ingestion to People table')

