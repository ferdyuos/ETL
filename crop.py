import csv

with open('/Users/ferdyuos/Applications/UWE/DMF_Assignment/dmf/bristol-air-quality-data.csv', 'r') as air_quality:
    read_air_quality = csv.DictReader(air_quality, delimiter=';')

    with open('crop.csv', 'w') as cropped_air_quality:
        columnslist = read_air_quality.fieldnames
        cropped_data = csv.DictWriter(cropped_air_quality, fieldnames=columnslist, delimiter=',')
        cropped_data.writeheader()
        
        for lines in read_air_quality:
            #pass the date format as string (no need to use datetime module)
            if lines['Date Time'] >= "2010-01-01T00:00:00+00:00":
                cropped_data.writerow(lines)      

    cropped_air_quality.close()
air_quality.close()

       

