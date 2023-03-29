#import csv modules
import csv

# station is declared as a dictionary
stations = {"188": "AURN Bristol Centre",
            "203": "Brislington Depot",
            "206": "Rupert Street",
            "209": "IKEA M32",
            "213": "Old Market",
            "215": "Parson Street School",
            "228": "Temple Meads Station",
            "270": "Wells Road",
            "271": "Trailer Portway P&R",
            "375": "Newfoundland Road Police Station",
            "395": "Shiner's Garage",
            "452": "AURN St Pauls",
            "447": "Bath Road",
            "459": "Cheltenham Road \ Station Road",
            "463": "Fishponds Road",
            "481": "CREATE Centre Roof",
            "500": "Temple Way",
            "501": "Colston Avenue"
        }

# open file to read and the file to write
with open('/Users/ferdyuos/Applications/UWE/DMF_Assignment/crop.csv') as cropped_air_quality: 
    #use dictreader to read file and write file
    #when using csv.dictreader, pass whats read as a list to fieldnames
    read_cropped_data = csv.DictReader(cropped_air_quality, delimiter=',')
    line_number = 1 # to know present line number

    with open('/Users/ferdyuos/Applications/UWE/DMF_Assignment/dmf/clean.csv', 'w') as clean_air_quality:
        columnslist = read_cropped_data.fieldnames
        read_clean_data = csv.DictWriter(clean_air_quality, fieldnames=columnslist, delimiter=',')
        read_clean_data.writeheader()
        for lines in read_cropped_data:
            
            #if the (looped siteid column is empty) , (looped siteid exists as a key in stations dictionary)
            #and if (value of the looped siteid in stations dictionary equals to the looped location)
            if lines['SiteID'] != '' and (lines['SiteID'] in stations.keys()) and stations[lines['SiteID']]== lines['Location']: 
                read_clean_data.writerow(lines)

            # to output dud values and mismatch
            elif lines['SiteID'] == '':
                print(f"in Line Number {line_number} Site ID is empty")
            
            elif lines['SiteID'] not in stations.keys():
                #if looped siteid exists as a key in stations dictionary
                print(f"in Line Number {line_number} Site ID {lines['SiteID']} does not match {lines['Location']}")

            elif stations[lines['SiteID']] != lines['Location']:    #if (value of the looped siteid in stations dictionary equals to the looped location)
                print("\n")
                print(f"in Line Number {line_number} Site ID {lines['SiteID']} did not match {lines['Location']}")
                print("\n")
            line_number += 1
    clean_air_quality.close() 
cropped_air_quality.close() 