from datetime import *
import calendar
import json
import random
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
print "Sections are: ",config.sections()

RoadList = {}
FinalRoadList=[]




for each_section in config.sections():
    for (each_key, each_val) in config.items(each_section):
        if each_key in 'speed':
            RoadList['Speed']=each_val.encode('utf-8');
        if each_key in 'capacity':
            RoadList['Capacity']=each_val.encode('utf-8');
        if each_key not in 'speed' and  each_key not in 'capacity':
            RoadList['Street Name']=each_key.encode('utf-8');
            x=each_val.encode('utf-8')
            Directions=x.split(',')
            for i in range(len(Directions)):
                Directions[i]=Directions[i].strip()
            print Directions
            RoadList['Directions']=Directions[0]
            print RoadList
            FinalRoadList.append(RoadList.copy())
            if(len(Directions)>1):
                RoadList['Directions']=Directions[1]
                FinalRoadList.append(RoadList.copy())
                print RoadList

print len(FinalRoadList)


with open('StreetList.json', 'w') as fp:
    json.dump(FinalRoadList, fp, sort_keys=True, indent=4)

with open('StreetList.json', 'r') as fp:
    data = json.load(fp)

congestion = None
d1 = date(2016, 01, 01)  # start date
d2 = date(2017, 01, 01)  # end date
finalList = []



delta=d2-d1
print delta.days


FinalTrafficlist=[]
file=open('sample.txt','a')

for oneDay in range(delta.days ):
    print oneDay
    for x in data:
            if x['Directions'] in 'North' or x['Directions'] in 'West':
                for hour in range(0, 24):
                    theDate = d1 + timedelta(days=oneDay)
                    theDay = calendar.day_name[theDate.weekday()]
                    if hour >= 0 and hour < 7 and theDay:
                         num_of_vehicle = random.randint(0, 20)

                    elif hour > 7 and hour < 10:
                         num_of_vehicle = random.randint(65, 85)

                    elif hour > 10 and hour < 16:
                         num_of_vehicle = random.randint(25, 45)

                    elif hour > 16 and hour < 18:
                         num_of_vehicle = random.randint(45, 65)

                    else:
                         num_of_vehicle = random.randint(20, 40)

                    if num_of_vehicle > 65:
                         congestion = True  # True
                    else:
                         congestion = False  # False

                    x['Date']=theDate.strftime('%Y-%m-%d')
                    x['Day']=theDay
                    timeStamp = datetime.strptime(str(hour), "%H").time()
                    print timeStamp
                    x['Time']= str(timeStamp)
                    x['Num of Vehicle']=num_of_vehicle
                    x['Congestion']=congestion
                    x['Capacity']=100 ## Need to change this
                    print x
                    FinalTrafficlist.append(x.copy())
                    file.write(str(x))
                    file.write(",")
            else:
                print "Found south or East! ",x['Street Name']
                for hour in range(0, 24):
                    theDate = d1 + timedelta(days=oneDay)
                    theDay = calendar.day_name[theDate.weekday()]
                    if hour >= 0 and hour < 7 and theDay:
                        num_of_vehicle = random.randint(0, 20)

                    elif hour > 7 and hour < 10:
                        num_of_vehicle = random.randint(45, 65)

                    elif hour > 10 and hour < 16:
                        num_of_vehicle = random.randint(25, 45)

                    elif hour > 16 and hour < 18:
                        num_of_vehicle = random.randint(65, 85)

                    else:
                        num_of_vehicle = random.randint(20, 40)

                    if num_of_vehicle > 65:
                        congestion = True  # True
                    else:
                        congestion = False  # False

                    x['Date'] = theDate.strftime('%Y-%m-%d')
                    x['Day'] = theDay
                    timeStamp = datetime.strptime(str(hour), "%H").time()
                    print timeStamp
                    x['Time'] = str(timeStamp)
                    x['Num of Vehicle'] = num_of_vehicle
                    x['Congestion'] = congestion
                    print x
                    FinalTrafficlist.append(x.copy())
                    file.write(str(x))
                    file.write(",")


file.close()

with open('test.json', 'w') as fp:
    json.dump(FinalTrafficlist, fp, sort_keys=True, indent=4)

print len(FinalTrafficlist)









#            data_dict = {'Date': theDate.strftime('%Y-%m-%d'),
#                     'Day': theDay,
#                      'Time': hour,
#                      'Num Of Vehicle': num_of_vehicle,
#                      'Speed Limit': speed_limit,
#                      'Congestion': congestion}
#         finalList.append(data_dict)
# infile = open("sanJoseStreets.txt", "r")
#
# with open("allStreet_oneMonth.txt", 'w') as outfile:
#     json.dump(finalList, outfile)
#
#





## If South:



