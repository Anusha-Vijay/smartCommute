import json
from pyspark.sql.types import DateType
import pandas
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from pyspark.sql import SparkSession
import sys
import collections

reject=['blvd','street','avenue','road','drive','expressway']

spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.routesWithTime").config(
            "spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.routesWithTime").getOrCreate()
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.routesWithTime").load()
df.cache()


sparkRoutes = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.allStreetsCongestion").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.trafficDataSet").getOrCreate()
dfRoutes = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.allStreetsCongestion").load()
dfRoutes.cache()



def getRoute(id):
    route=[]
    routeDetails = {}
    stepsDF = df.select("Steps").filter(df["Id"] == id).collect()
    for row in stepsDF[0][0]:
        routeDetails["Street Name"] = row[0].lower()
        routeDetails["Direction"] = row[1]
        route.append(routeDetails.copy())
    return route

def getTravelTime(id):
    travelTime = df.select("Travel Time").filter(df["Id"] == id).collect()
    if(len(travelTime)==0):
        return -1
    else:
        return travelTime[0][0]

def getAllID():
    routeID = [i.Id for i in df.select('Id').distinct().collect()]
    return routeID

def get24HourTravelTime(route,travelTime,day):
    response={}
    for i in range(0,24):
        if(i<10):
            key="0"+str(i)+":00:00"
        else:
            key = str(i) + ":00:00"
        response[key]=0.0
    for item in route:
        streetName = item["Street Name"]
        direction = item["Direction"]
        streetDF = dfRoutes.filter((dfRoutes["Street Name"] == streetName) & (dfRoutes["Directions"] == direction) & (dfRoutes["Day"]==day))
        ##streetDF=streetDF.select("Street Name","Day","Time","Num of Vehicle").groupby("Time").avg().sort("Time",ascending=True)
        rows=streetDF.select("Time","avg(Num of Vehicle)").collect()
        for row in rows:
            response[row[0]] = ((response[row[0]]+row[1])/2)
    travelTime = [int(s) for s in travelTime.split() if s.isdigit()]
    travelTime=travelTime[0]
    for k,v in response.iteritems():
        t=travelTime
        if(v>30.00):
            t=travelTime + ((v/100)*t)
        response[k]=round(t,2)
    return response


def get24HourCongestion(route,day):
    response={}
    for i in range(0,24):
        if(i<10):
            key="0"+str(i)+":00:00"
        else:
            key = str(i) + ":00:00"
        response[key]=0.0
    for item in route:
        streetName = item["Street Name"]
        direction = item["Direction"]
        streetDF = dfRoutes.filter((dfRoutes["Street Name"] == streetName) & (dfRoutes["Directions"] == direction) & (dfRoutes["Day"]==day))
        ##streetDF=streetDF.select("Street Name","Day","Time","Num of Vehicle").groupby("Time").avg().sort("Time",ascending=True)
        rows=streetDF.select("Time","avg(Num of Vehicle)").collect()
        for row in rows:
            x = ((response[row[0]]+row[1])/2)
            response[row[0]] = round(x,2)
    return response

def getID(source,destination):
    x = df.select("Id").filter((df["Source"]==source) & (df["Destination"]==destination)).collect()
    if(len(x)==0):
        exit(-1)
    return x[0][0]

if __name__:
    #Needs
    #Source,Destination,Day,Type
    if(len(sys.argv)<5):
        print "Post Body should include Source, Destination,Day"
    source=sys.argv[1]
    destination=sys.argv[2]
    day=sys.argv[3]
    type=sys.argv[4]
    id = getID(source, destination)
    if(id==-1):
        exit(-1)
    route=getRoute(id=id)
    travelTime = getTravelTime(id)
    if(sys.argv[4]=='1'):
        response=get24HourCongestion(route,day)
        temp = {}
        x = []
        with open("get24HourCongestion.txt","w") as f:
            for k,v in response.iteritems():
                temp["label"]=k[0:2]
                temp["value"]=v
                x.append(temp.copy())
            x = sorted(x, key=lambda k: k['label'])
            f.write(",".join(map(lambda x: str(x), x)))
            f.close()
    else:
        response=get24HourTravelTime(route,travelTime,day)
        temp={}
        x=[]
        with open("get24HourTravelTime.txt","w") as f:
            for k,v in response.iteritems():
                temp["label"]=k[0:2]
                temp["value"]=v
                x.append(temp.copy())
            x = sorted(x, key=lambda k: k['label'])
            f.write(",".join(map(lambda x: str(x), x)))
            f.close()






























def caller(x):
    sys.argv=x
    if(len(sys.argv)<5):
        print "Post Body shuld include Source, Destination,Day"
    source=sys.argv[1]
    destination=sys.argv[2]
    day=sys.argv[3]
    type=sys.argv[4]
    id = getID(source, destination)
    route=getRoute(id=id)
    travelTime = getTravelTime(id)
    if(type=='1'):
        response=get24HourCongestion(route,day)
        temp = {}
        x = []
        with open("get24HourCongestion.txt","w") as f:
            for k,v in response.iteritems():
                temp["label"]=k[0:2]
                temp["value"]=v
                x.append(temp.copy())
            x = sorted(x, key=lambda k: k['label'])
            print "-------------CONGESTION VS TIME------------"
            for item in x:
                print item['label'],":",item['value']
            f.write(",".join(map(lambda x: str(x), x)))
            f.close()
    else:
        response=get24HourTravelTime(route,travelTime,day)
        temp={}
        x=[]
        with open("get24HourTravelTime.txt","w") as f:
            for k,v in response.iteritems():
                temp["label"]=k[0:2]
                temp["value"]=v
                x.append(temp.copy())
            x = sorted(x, key=lambda k: k['label'])
            print "-------------TRAVEL TIME VS TIME------------"
            for item in x:
                print item['label'], ":", item['value']
            f.write(",".join(map(lambda x: str(x), x)))
            f.close()

