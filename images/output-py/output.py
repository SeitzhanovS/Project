
import json
import sqlalchemy

# connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@localhost/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
#Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)


# output the table to a JSON file
with open('data/summary_output.json', 'w+') as json_file:
  rows = connection.execute("""SELECT country, count(given_name)
FROM Places pl
INNER JOIN People pp ON pl.city = pp.place_of_birth
GROUP BY country""")
  rows = [{'country': row[0], 'count': row[1]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))