#!/usr/bin/env python

import sqlalchemy
import csv
import pandas as pd
import json

engine = sqlalchemy.create_engine("mysql://codetest:swordfish@localhost/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

def smoketest():
  Plc = connection.execute("""SELECT COUNT(*) FROM Places""")
  print('Places count:',[row[0] for row in Plc])
  Pec = connection.execute("""SELECT COUNT(*) FROM People""")
  print('People count',[row[0] for row in Pec])

def places_ingestion():

  print('Starting ingestion to Places table')
  Places = sqlalchemy.schema.Table('Places', metadata, autoload=True, autoload_with=engine)
  connection.execute("""SET FOREIGN_KEY_CHECKS = 0""")
  connection.execute("""TRUNCATE TABLE Places""")
  connection.execute("""SET FOREIGN_KEY_CHECKS = 1""")
  with open('data/places.csv', encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    next(reader)
    for row in reader:
      connection.execute(Places.insert().values(city=row[0], county=row[1], country=row[2]))

  print('Finished ingestion to Places table')

def people_ingestion():

  print('Starting ingestion to People table')
  People = sqlalchemy.schema.Table('People', metadata, autoload=True, autoload_with=engine)

  connection.execute("""SET FOREIGN_KEY_CHECKS = 0""")
  connection.execute("""TRUNCATE TABLE People""")
  connection.execute("""SET FOREIGN_KEY_CHECKS = 1""")
  with open('data/people.csv', encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    lst = list()
    next(reader)
    i = 1
    for row in reader:
      lst.append(row)
      if (len(lst) % 1000 == 0):
        print(i * 1000, 'Rows read and inserted')
        i = i + 1
        df = pd.DataFrame(lst, columns=['given_name', 'family_name', 'date_of_birth', 'place_of_birth'])
        df.to_sql('People', connection, if_exists='append', index=False)
        lst.clear()
  print('Finished ingestion to People table')


if sqlalchemy.inspect(engine).has_table("Places"):

  try:
    places_ingestion()
  except sqlalchemy.exc.OperationalError as e:
    print('Database connection error')
    print('Error text : ', str(e))

else:
  connection.execute("""CREATE TABLE Places(
          city varchar(255),
          county varchar(255),
          country varchar(255),
          PRIMARY KEY (city)
    )""")
  places_ingestion()


if sqlalchemy.inspect(engine).has_table("People"):

  try:
    people_ingestion()
  except sqlalchemy.exc.OperationalError as e:
    print('Database connection error')
    print('Error text : ', str(e))

else:
  connection.execute("""CREATE TABLE People(
    given_name varchar(255),
    family_name varchar(255),
    date_of_birth date,
    place_of_birth varchar(255),
    FOREIGN KEY (place_of_birth)
    REFERENCES Places (city)
  )""")
  people_ingestion()

smoketest()
print('Ingestion complete')



