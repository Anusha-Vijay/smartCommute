import json
from pyspark.sql.types import DateType
import pandas
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from pyspark.sql import SparkSession
import sys
reject=['blvd','street','avenue','road','drive','expressway']


## This is to get the DB
def getAllCongestionVsTime():
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.trafficDataSet").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.trafficDataSet").getOrCreate()
    df_new = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.trafficDataSet").load()
    df_new.cache()
    newDF = df_new.select(df_new["Street Name"], df_new["Directions"], df_new["Day"], df_new["Time"], df_new["Num of Vehicle"]).groupby("Street Name", "Directions", "Day", "Time").avg().sort(df_new["Street Name"], df_new["Directions"], df_new["Day"], df_new["Time"], ascending=True)
    newDF.show()
    newDF=newDF.toPandas()
    newDF.to_csv('/home/student/ProjectARIMA/CMPE295/PredictionServer/newVersion/AllStreetCongestionVs24Time.csv')

def cleanInputs(streetName):
    streetName=streetName.lower()
    streetName=streetName.split()
    resultwords = [word for word in streetName if word.lower() not in reject]
    result = ' '.join(resultwords)
    print "Result",result
    return result

def getCongestionVsTime(streetName,Direction,Day):
    response=[]
    item={}
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.allStreetsCongestion").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.allStreetsCongestion").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.allStreetsCongestion").load()
    #df.show()
    cmd = "%" + streetName.strip() + "%"
    #print cmd
    df_new = df.filter(df["Street Name"].like(cmd) & (df["Directions"] == Direction) & (df["Day"] == Day)).sort("Time")
    l = df_new.select("Time", "avg(Num of Vehicle)").sort("Time").collect()
    for row in l:
        item["label"] = row[0]
        item["value"] = row[1]
        response.append(item.copy())
    print response
    with open("getCongestionVsTime.txt","w") as f:
        f.write(str(response))
    #return response

if __name__:
    print sys.argv
    streetName=cleanInputs(sys.argv[1])
    getCongestionVsTime(streetName,sys.argv[2],sys.argv[3])
