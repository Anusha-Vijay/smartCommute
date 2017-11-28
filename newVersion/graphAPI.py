import json
from pyspark.sql.types import DateType
import pandas
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from pyspark.sql import SparkSession
import sys



def Prediction(jsonBody):
    steps = jsonBody["Steps"]
    startTime = jsonBody["Time"]
    respObj = {}
    print startTime
    jsonResponse = []
    print len(steps)
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.trafficDataSet").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.trafficDataSet").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.trafficDataSet").load()
    df = df.withColumn('Date', df['Date'].cast(DateType()))
    df.printSchema()
    df.take(2)
    for i in steps:
        print "Getting information:",i["Street Name"].lower(), "  Direction:",i["Direction"]
        print type(i["Street Name"])
        jsonIndex = i["Street Name"]
        print jsonIndex
        streetName=str(i["Street Name"].strip().lower())
        direction=str(i["Direction"]).strip()
        print type(streetName)
        cmd = "%" + startTime.strip() + "%"
        df_new = df.filter((df["Street Name"] == streetName) & (df["Directions"] == direction))
        df_new=df_new.filter(df_new.Time.like(cmd))
        #df_new = df.filter((df["Street Name"] == i["Street Name"].lower()) & (df["Time"] == startTime) & (df["Directions"]== i["Direction"].strip()) & (df["Day"] == day.strip()))
        df_new.show()
        df_new = df_new.toPandas()
        print len(df_new)
        if(len(df_new)==0):
            continue
        df_new['Time'] = pandas.to_datetime(df_new['Time']).dt.time
        temp = df_new[["Num of Vehicle", "Time"]]
        temp = df_new[["Num of Vehicle", "Time"]]
        d = temp["Num of Vehicle"]
        d = list(d)
        index = list(temp["Time"])
        [float(i) for i in d]
        temp = pandas.Series(data=d, index=index, dtype=float)
        size = int(len(temp) * 0.90)
        train, test = temp[0:size], temp[size:len(temp)]
        mean = test.mean()
        print "Mean is: ", test.mean()
        print test[1:5]
        print type(test)
        otest=[]
        history = [x for x in train]
        predictions = list()
        for t in range(len(test)):
            model = ARIMA(history, order=(5, 1, 0))
            model_fit = model.fit(disp=0)
            output = model_fit.forecast()
            yhat = output[0]
            predictions.append(yhat)
            obs = test[t:t + 1]
            history.append(obs)
            print('predicted=%f, expected=%f' % (yhat, obs))
            print yhat[0]
            otest.append(int(yhat))
        error = mean_squared_error(test, predictions)
        print('Test MSE:', error)
        val=sum(otest) / float(len(otest))
        print "Mean of predicted is:",val
        print type(respObj)
        print val
        respObj["CongestionRate"] = val
        respObj["StreetName"] = jsonIndex
        jsonResponse.append(respObj.copy())
        print jsonResponse
    print "Final Output is::::",jsonResponse
    for item in jsonResponse:
        x=x+item["CongestionRate"]
    return (x/len(jsonResponse))



def getAllID():
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.routesWithTime").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.routesWithTime").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.routesWithTime").load()
    routeID=[i.Id for i in df.select('Id').distinct().collect()]
    return routeID

def getRouteDetails(id):
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.routesWithTime").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.routesWithTime").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.routesWithTime").load()
    df.printSchema()
    df.show()
    routeDetails={}
    stepsDF = df.filter(df["ID"] == id)
    print stepsDF.select("Steps").show()
    print type(stepsDF)
    stepsDF=stepsDF.toPandas()
    steps=stepsDF["Steps"]
    steps=list(steps)
    steps=steps[0]
    print steps
    requestDetails=[]
    for item in steps:
        routeDetails["Street Name"] = item[0]
        routeDetails["Direction"] = item[1]
        requestDetails.append(routeDetails.copy())
    print requestDetails
    primaryRequest={}
    primaryRequest["Steps"] = requestDetails
    primaryRequest["Source"]=stepsDF["Source"]
    primaryRequest["Destination"] = stepsDF["Destination"]
    primaryRequest["Travel Time"] = stepsDF["Travel Time"]
    return primaryRequest


def get24HourCongestion(RouteDetails):
    responseList=[]
    temp={}
    for i in range (0,24):
        if (i<10):
            currentTime = "0"+str(i) + ":00:00"
        else:
            currentTime=str(i)+":00:00"
        print currentTime
        ##Get Congestion Percent
        RouteDetails["Time"]=currentTime
        Congestion=Prediction(RouteDetails)
        print "---------------------------"
        TravelTime = RouteDetails["Travel Time"]
        TravelTime = TravelTime[0]
        TravelTime=[int(s) for s in TravelTime.split() if s.isdigit()]
        TravelTime=TravelTime[0]
        if (Congestion>20.00):
            TravelTime=TravelTime+(Congestion*TravelTime/100)
        temp["Time"]=currentTime
        temp["Travel Time"]=TravelTime
        responseList.append(TravelTime)
    print "-----Final---"
    print responseList
    return responseList

if __name__ == "__main__":
    routeList=[1]
    finalList=[]
    for routeID in routeList:
        RouteDetails=getRouteDetails(routeID)
        print "Final Primary Request is:\n",RouteDetails
        x=get24HourCongestion(RouteDetails)
        finalList.append(x)

    print "This is what needs to get written into a file!"
    print finalList
    with open('24TravelTimeRoute.txt', 'w') as fp:
        fp.write(finalList)
