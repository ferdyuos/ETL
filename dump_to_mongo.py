import mysql.connector
from clean import stations
import pymongo

#mysql connection uing msql connector
conn = mysql.connector.connect(
  host="localhost",
  user="obinna",
  password="obinnadmf2021"
)
cur = conn.cursor()
cur.execute("USE `pollution-db2`")

#mongodb connection using pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
collection = mydb["readings"]

my_list = [] #list to hold dictionaries

#let user pick their choice of station they want to model
choice = input("choose station you want to model")

if choice not in stations:
  print("choose correct station") 
else:
  select_site_id= """SELECT * FROM stations INNER JOIN readings ON stations.site_id = readings.stations_site_id WHERE site_id = %s LIMIT 100"""
  sitte = (choice,)
  cur.execute(select_site_id, sitte)
  rows = cur.fetchall()
  for row in rows:
    dic_readings = {'site_id': row[0], 'location': row[1] , 'geo_point_2d': row[2], 
    'date_time': str(row[4]), 'date_start': str(row[5]) , 'instrument_type': row[6] , 'nox': str(row[7]), 
    'no2': str(row[8]) , 'no': str(row[9]), 'pm10': str(row[10]), 'nvpm10': str(row[11]) , 'vpm10': str(row[12]), 
    'nvpm2.5': str(row[13]), 'pm2.5': str(row[14]) , 'vpm2.5': str(row[15]) , 'co': str(row[16]), 'o3': str(row[17]) , 
    'so2': str(row[18]), 'temperature': str(row[19]), 'rh': str(row[20]) , 'air_pressure': str(row[21]), 'current': row[22] , 
    'date_end': str(row[23])}
    
   
    my_list.append(dic_readings)  #appends dictionary to list

x = collection.insert_many(my_list) # inserts array of dictionaries to resdings collection
#prints out all documents in readings collection
for x in collection.find():
  print(x)

conn.close()



