import csv
import itertools

#function to insert n-1 rows given the key-value pair in kwargs
def first(**kwargs):
    values = ["'%s'" % v for v in kwargs.values()]
    sql = list()
    sql.append("(")
    sql.append(", ".join(values))
    sql.append(")")
    sql.append(",")
    return "".join(sql)

#function to insert nth row given the key-value pair in kwargs
def last(**kwargs):
    
    values = ["'%s'" % v for v in kwargs.values()]
    sql = list()
    sql.append("(")
    sql.append(", ".join(values))
    sql.append(")")
    sql.append(";")
    return "".join(sql)

user_input = int(input("how many records do you want to create a query for?"))

with open('/Users/ferdyuos/Applications/UWE/DMF_Assignment/try/crop.csv', 'r+') as air_quality:
    clean_read_air_quality = csv.DictReader(air_quality, delimiter=',')
    # limit the data to 1st 100
    sliced_data= itertools.islice(clean_read_air_quality,user_input)
    
    with open('Insert-100.sql', 'w') as sql:
        sql.writelines("INSERT IGNORE INTO `readings` (date_time,nox,no2,no,pm10,nvpm10,vpm10,nvpm2.5,pm2.5,vpm2.5,co,o3,so2,temperature,rh,air_pressure,location,geo_point_2d,date_start,date_end,current,instrument_type) VALUES ")
        sql.writelines("\n")
        count=1
        for lines in sliced_data:
            
            # if 1st- (n-1) record, the first() is applied else, use the last() to append the ;
            if count <user_input:
                sql.writelines(first(**lines))
                sql.writelines("\n")
            elif count ==user_input:
                 sql.writelines(last(**lines))
            count +=1
    sql.close()   
air_quality.close()



