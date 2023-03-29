#import modules to be used
import mysql.connector
import csv
from datetime import datetime

try: 
  conn = mysql.connector.connect(
    host="localhost",
    user="obinna",
    password="obinnadmf2021"
  )

  #drop and create databases if exists
  cur = conn.cursor()
  cur.execute("DROP DATABASE IF EXISTS `pollution-db2`")
  cur.execute("CREATE DATABASE IF NOT EXISTS `pollution-db2`")
  # cur.execute("CREATE DATABASE `pollution-db2`")
  # empty list to hold records
  records = [];

  # read in the csv file as a list one at a time using csv reader
  with open('/Users/ferdyuos/Applications/UWE/DMF_Assignment/try/clean.csv','r') as csvfile:
      reader = csv.reader(csvfile, delimiter=',')
      for row in reader:
          records.append(row)
      
  # records[] is now a list of lists

  # get rid of the header row
  records.pop(0)

  # get a database handle
  cur.execute("USE `pollution-db2`")

  # defining the SQL for the tables
  stations_table = """CREATE TABLE `stations`
  (
    `site_id` INT(11) NOT NULL,
    `location` VARCHAR(45) NOT NULL,
    `geo_point_2d` VARCHAR(45) NOT NULL,
    UNIQUE INDEX `site_id_UNIQUE` (`site_id` ASC) VISIBLE,
    UNIQUE INDEX `geo_point_2d_UNIQUE` (`geo_point_2d` ASC) VISIBLE,
    UNIQUE INDEX `location_UNIQUE` (`location` ASC) VISIBLE,
    PRIMARY KEY (`site_id`)
  )
  """


  readings_table = """CREATE TABLE `readings`
  (
    `id` BIGINT(10) NOT NULL AUTO_INCREMENT,
    `date_time` DATETIME NOT NULL,
    `date_start` DATETIME NOT NULL,
    `instrument_type` VARCHAR(45) NOT NULL,
    `nox` DECIMAL(30,10) NOT NULL,
    `no2` DECIMAL(30,10) NOT NULL,
    `no` DECIMAL(30,10) NOT NULL,
    `pm10` DECIMAL(30,10) NOT NULL,
    `nvpm10` DECIMAL(30,10) NOT NULL,
    `vpm10` DECIMAL(30,10) NOT NULL,
    `nvpm2.5` DECIMAL(30,10) NOT NULL,
    `pm2.5` DECIMAL(30,10) NOT NULL,
    `vpm2.5` DECIMAL(30,10) NOT NULL,
    `co` DECIMAL(30,10) NOT NULL,
    `o3` DECIMAL(30,10) NOT NULL,
    `so2` DECIMAL(30,10) NOT NULL,
    `temperature` REAL NOT NULL,
    `rh` INT NOT NULL,
    `air_pressure` INT NOT NULL,
    `current` TEXT(5) NOT NULL,
    `date_end` DATETIME NOT NULL,
    `stations_site_id` INT(11) NOT NULL,
    PRIMARY KEY (`id`),
    INDEX `fk_readings_stations_idx` (`stations_site_id` ASC) VISIBLE,
    CONSTRAINT `fk_readings_stations`
      FOREIGN KEY (`stations_site_id`)
      REFERENCES `pollution-db2`.`stations` (`site_id`)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION
  )
  """

  schema_table = """CREATE TABLE `schema`
  (
    `measure` VARCHAR(45) NOT NULL,
    `description` VARCHAR(100) NOT NULL,
    `unit` VARCHAR(45) NOT NULL,
    UNIQUE INDEX `measure_UNIQUE` (`measure` ASC) VISIBLE,
    PRIMARY KEY (`measure`)
  )
  """


  #current timestamp before creating stations table
  before_stations = datetime.now()
  print(f"Time before creating stations table is: {before_stations}")
  cur.execute(stations_table)
  #current timestamp after creating stations table
  after_stations = datetime.now()
  print(f"Time after creating stations table is: {after_stations}")
  print("\n")


  #current timestamp before creating readings table
  before_readings = datetime.now()
  print(f"Time before creating readings table is: {before_readings}")
  cur.execute(readings_table)
  #current timestamp after creating readings table
  after_readings = datetime.now()
  print(f"Time after creating readings table is: {after_readings}")
  print("\n")


  #current timestamp before creating schema table
  before_schema = datetime.now()
  print(f"Time before creating schema table is: {before_schema}")
  cur.execute(schema_table)
  #current timestamp after creating schema table
  after_schema = datetime.now()
  print(f"Time after creating schema table is: {after_schema}")
  print("\n")
      
  
  #current timestamp before inserting into database
  before_insert = datetime.now()
  print(f"Time before inserting into tables is: {before_insert}")
  print("\n")

  for row in records:
              

    conn.autocommit = False
    

    stations_insert =  """INSERT IGNORE INTO `stations` values(%s, %s, %s)"""
    # stations_insert =  """INSERT IGNORE INTO `stations` values('alero','nkem','uwam')"""
    stations_values = (row[4],row[17], row[18])
    # stations_values = ('alero','nkem','uwam')
    cur.execute(stations_insert, stations_values)

   
    select_site_id= """SELECT site_id FROM `stations` WHERE site_id = %s"""
    sitte = (row[4],) #203
    cur.execute(select_site_id, sitte)
    station_id = cur.fetchone()[0] 
    readings_insert =  """INSERT IGNORE INTO `readings` values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s,%s)"""
    readings_values = ("", row[0], row[19], row[22], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9], row[10], 
    row[11], row[12], row[13], row[14], row[15], row[16], row[21], row[20],station_id)
    cur.execute(readings_insert, readings_values)


    schema_insert = """INSERT IGNORE INTO `schema` VALUES (%s, %s,%s)"""
    schema_list = [("Date and time of measurement", "date time"), ("Concentration of oxides of nitrogen", "μg/m3"),
    ("Concentration of nitrogen dioxide", "μg/m3"), ("Concentration of nitric oxide", "μg/m3"), 
    ("Site ID for the station", "integer"), ("Concentration of particulate matter <10 micron diameter", "μg/m3"),
    ("Concentration of non - volatile particulate matter <10 micron diameter", "μg/m3"), 
    ("Concentration of volatile particulate matter <10 micron diameter", "μg/m3"),
    ("Concentration of non volatile particulate matter <2.5 micron diameter", "μg/m3"), 
    ("Concentration of particulate matter <2.5 micron diameter", "μg/m3"),
    ("Concentration of volatile particulate matter <2.5 micron diameter", "μg/m3"), ("Concentration of carbon monoxide", "μg/m3"), 
    ("Concentration of ozone", "μg/m3"),("Concentration of sulphur dioxide", "μg/m3"), ("Air temperature", "°C"),
    ("Relative Humidity", "%"),("Air Pressure", "mbar"),("Text Description of location", "text"),("Latitude and longitude", "geopoint"),
    ("The date monitoring started", "datetime"),("The date monitoring ended", "datetime"),
    ("Is the monitor currently operating", "text"),("Classification of the instrument", "text")
    ]
    schema_values = [
    ("Date Time",schema_list[0][0],schema_list[0][1]),("Nox",schema_list[1][0],schema_list[1][1]),
    ("No2",schema_list[2][0],schema_list[2][1]),("NO",schema_list[3][0],schema_list[3][1]),
    ("SiteID",schema_list[4][0],schema_list[4][1]),("PM10",schema_list[5][0],schema_list[5][1]),
    ("NVPM10",schema_list[6][0],schema_list[6][1]),("VPM10",schema_list[7][0],schema_list[7][1]),
    ("NVPM2.5",schema_list[8][0],schema_list[8][1]),("PM2.5",schema_list[9][0],schema_list[9][1]),
    ("VPM2.5",schema_list[10][0],schema_list[10][1]),("CO",schema_list[11][0],schema_list[11][1]),
    ("O3",schema_list[12][0],schema_list[12][1]),("SO2",schema_list[13][0],schema_list[13][1]),
    ("Temperature",schema_list[14][0],schema_list[14][1]),("RH",schema_list[15][0],schema_list[15][1]),
    ("Air Pressure",schema_list[16][0],schema_list[16][1]),("Location",schema_list[17][0],schema_list[17][1]),
    ("geo_point_2d",schema_list[18][0],schema_list[18][1]),("DateStart",schema_list[19][0],schema_list[19][1]),
    ("DateEnd",schema_list[20][0],schema_list[20][1]),("Current",schema_list[21][0],schema_list[21][1]),
    ("Instrument Type",schema_list[22][0],schema_list[22][1])
    ]


    cur.executemany(schema_insert, schema_values)


  after_insert = datetime.now()
  print(f"Time after inserting into tables is: {after_insert}")
  print("\n")

except BaseException as err:
  print(f"An error occured: {err}")


conn.commit()
conn.close()



