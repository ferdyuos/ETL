#!/usr/bin/env python
# coding: utf-8
# import required packages
import mysql.connector
from mysql.connector import Error
import pandas as pd
import csv


# create a connection to a mysql server
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


connection = create_server_connection("localhost", "obinna", "obinnadmf2021")


# Create a database
def drop_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database dropped successfully")
    except Error as err:
        print(f"Error: '{err}'")


# Create a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


drop_database_query = "DROP DATABASE IF EXISTS pollution_db5"
create_database_query = "CREATE DATABASE pollution_db5"
drop_database(connection, drop_database_query)
create_database(connection, create_database_query)


# connect to an exisiting database
def create_db_connection(host_name: object, user_name: object, user_password: object, db_name: object) -> object:
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


connection = create_db_connection("localhost", "obinna", "obinnadmf2021", "pollution_db5")


# data.head()

# function to execute queries
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# # sql statement to create site table
# create_site_table='''CREATE TABLE IF NOT EXISTS `pollution_db2`.`site` (siteID INT Primary Key, location char(50)Unique Key);'''


create_stations_table = '''CREATE TABLE `stations`
(
`site_id` INT(11) NOT NULL,
`location` VARCHAR(45) NOT NULL,
`geo_point_2d` VARCHAR(45) NOT NULL,
PRIMARY KEY (`site_id`)
);
'''

create_readings_table = '''CREATE TABLE `readings`
(
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `date_time` DATETIME NOT NULL,
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
    `date_start` DATETIME NOT NULL,
    `date_end` DATETIME NOT NULL,
    `current` TEXT(5) NOT NULL,
    `instrument_type` VARCHAR(45) NOT NULL,
    `site_id-fk` INT(11) NOT NULL,
    PRIMARY KEY (`id`)
);
'''

create_schema_table = '''CREATE TABLE `schema`
(
    `measure` VARCHAR(45) NOT NULL,
    `description` VARCHAR(100) NOT NULL,
    `unit` VARCHAR(45) NOT NULL,
    PRIMARY KEY (`measure`)
);
'''
alter_readings_table = ''' ALTER TABLE readings ADD FOREIGN KEY (`site_id-fk`) REFERENCES stations(site_id);
'''

# excute all sql statement to create tables
print('Creating tables....')
execute_query(connection, create_stations_table)
execute_query(connection, create_readings_table)
execute_query(connection, create_schema_table)
execute_query(connection, alter_readings_table)
print("Tables are created....")


try:
    records = []

    with open('clean.csv', 'r') as clean:

        reader = csv.reader(clean, delimiter=',')

        for row in reader:
            records.append(row)

    # remove header row

    records.pop(0)

except Error as e:
    print("Error while connecting to MySQL", e)


# sql statement to populate site table
try:
    cursor = connection.cursor()

    for row in records:

        #setting the autocommit flag to false
        connection.autocommit = False

        try:
            #insert into stations table
            stations_query = '''INSERT IGNORE INTO `stations` values(%s, %s, %s)'''
            stations_result = (row[4], row[17], row[18])
            cursor.execute(stations_query, stations_result)
        except Error as e:
            print("Error while connecting to MySQL", e)

        try:
            cursor.execute('SELECT * FROM `stations` WHERE site_id = %s', (row[4],))
            sid = cursor.fetchone()[0]
            #insert into readings table
            readings_query = '''INSERT IGNORE INTO readings values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            readings_result = ("", row[0], row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[19], row[20], row[21], row[22], sid)
            cursor.execute(readings_query, readings_result)
        except Error as e:
            print("Error while connecting to MySQL", e)


        try:
            schema_query = '''INSERT IGNORE INTO `schema` VALUES (%s, %s, %s)'''
            schema_result = [("Date Time", "Date and time of measurement", "datetime"), ("NOx","Concentration of oxides of nitrogen", "μg/m3"),
                             ("NO2", "Concentration of nitrogen dioxide", "μg/m3"),("NO", "Concentration of nitric oxide", "μg/m3"),
                             ("SiteID",  "Site ID for the station","integer"),("PM10", "Concentration of particulate matter <10 micron diameter", "μg/m3"),
                             ("NVPM10", "Concentration of non - volatile particulate matter <10 micron diameter", "μg/m3"),
                             ("VPM10", "Concentration of volatile particulate matter <10 micron diameter", "μg/m3"),
                             ("NVPM2.5", "Concentration of non volatile particulate matter <2.5 micron diameter", "μg/m3"),
                             ("PM2.5", "Concentration of particulate matter <2.5 micron diameter", "μg/m3"),
                             ("VPM2.5", "Concentration of volatile particulate matter <2.5 micron diameter", "μg/m3"),("CO", "Concentration of carbon monoxide", "mg/m3"),
                             ("O3", "Concentration of ozone", "μg/m3"),("SO2", "Concentration of sulphur dioxide", "μg/m3"),("Temperature", "Air temperature", "°C"),
                             ("RH", "Relative Humidity", "%"),("Air Pressure", "Air Pressure", "mbar"),("Location", "Text description of location", "text"),
                             ("geo_point_2d", "Latitude and longitude", "geo point"),("DateStart", "The date monitoring started", "datetime"),
                             ("DateEnd", "The date monitoring ended", "datetime"),("Current", "Is the monitor currently operating", "text"),
                             ("Instrument Type", "Classification of the instrument", "text")]
            cursor.executemany(schema_query, schema_result)

        except Error as e:
            print("Error while connecting to MySQL", e)

    connection.commit()
    connection.close()
    print("done dumping data to database")


except Error as e:
    print("Error while connecting to MySQL", e)


