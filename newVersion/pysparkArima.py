import json
from pyspark.sql.types import DateType
import pandas
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from pyspark.sql import SparkSession
import sys

def Prediction(jsonBody):
    steps=jsonBody["Steps"]
    startTime=jsonBody["Time"]
    day = jsonBody["Day"]
    respObj={}
    print startTime
    print day
    jsonResponse=[]
    print len(steps)
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.trafficDataSet").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.trafficDataSet").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/dataStore.trafficDataSet").load()
    df = df.withColumn('Date', df['Date'].cast(DateType()))
    df.printSchema()
    df.take(2)
    df.cache()
    for i in steps:
        print "Getting information:",i["Street Name"].lower(), "  Direction:",i["Direction"]
        print type(i["Street Name"])
        jsonIndex = i["Street Name"]
        print jsonIndex
        streetName=str(i["Street Name"].strip().lower())
        direction=str(i["Direction"]).strip()
        print type(streetName)
        cmd = "%" + startTime.strip() + "%"
        df_new = df.filter((df["Street Name"] == streetName) & (df["Directions"] == direction) & (df["Day"] == day))
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
            ##print"------>",type(yhat)
            print yhat[0]
            ##print "----->",type(obs)
            otest.append(int(yhat))
        error = mean_squared_error(test, predictions)
        print('Test MSE:', error)
        val=sum(otest) / float(len(otest))
        print "Mean of predicted is:",val
        print type(respObj)
        print val
        respObj["CongestionRate"]=str(round(val,2))+"%"
        respObj["StreetName"]=jsonIndex
        jsonResponse.append(respObj.copy())
        print jsonResponse

    print "Final Output is::::",jsonResponse
    print "Writing into congestionresult.json file"
    with open("Congestionresult.json", "w") as f:
        f.write(str(jsonResponse))
        f.close()

    #
    # with open('Congestionresult.json', 'w') as fp:
    #     json.dump(jsonResponse, fp, sort_keys=True, indent=4)


def getID(primaryRequest):
    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://127.0.0.1/dataStore.routesWithTime").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dataStore.routesWithTime").getOrCreate()
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/dataStore.routesWithTime").load()
    srccmd = "%" +str(primaryRequest["Source"]).strip()+"%"
    dstcmd = "%" + str(primaryRequest["Destination"]).strip() + "%"
    df_new = df.filter(df.Source.like(srccmd))
    df_new= df_new.filter(df_new.Destination.like(dstcmd))
    return df_new.select("Id").collect()[0][0]

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
    return requestDetails

if __name__ == "__main__":
    print "Recived\n--------------\n"
    print sys.argv
    if(len(sys.argv) < 5):
        print "Invalid number of inputs"
        print "Usage: Source: <>, Destination: <>, Time: <>, Day: <>"
        print "Recived\n--------------\n"
        print sys.argv
        exit(-1)
    primaryRequest={}
    primaryRequest["Source"] = sys.argv[1]
    primaryRequest["Destination"] = sys.argv[2]
    primaryRequest["Time"] = sys.argv[3]
    primaryRequest["Day"] = sys.argv[4]
    print primaryRequest
    id=getID(primaryRequest)
    RouteDetails=getRouteDetails(id)
    primaryRequest["Steps"] = RouteDetails
    print "Final Primary Request is:\n",primaryRequest
    json=Prediction(primaryRequest)
