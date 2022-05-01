#!/usr/bin/env python

import json
import sqlalchemy

# connect to the database
try:
  engine = sqlalchemy.create_engine("mysql://codetest:swordfish@localhost/codetest")
  connection = engine.connect()

  metadata = sqlalchemy.schema.MetaData(engine)

  query = """
  SELECT country, count(given_name)
    FROM Places pl
    INNER JOIN People pp ON pl.city = pp.place_of_birth
    GROUP BY country
  """


  with open('data/summary_output.json', 'w+') as json_file:
    rows = connection.execute(query)
    rows = [{'country': row[0], 'count': row[1]} for row in rows]
    json.dump(rows, json_file, separators=(',', ':'))

except sqlalchemy.exc.OperationalError as e:
  print('Database connection error')
  print('Error text : ',str(e))