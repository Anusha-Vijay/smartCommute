from flask import Flask
from flask import request
import json
import os
import pandas
import ast
import commands
from singleCongestionData import singleCongestionDF
from Congestionvs24hoursCache import congestionDF


Places = {
    "casa verde street": 'Casa Verde Street',
    'alum rock park': 'Alum Rock Park',
    'san jose state university': 'San Jose State University',
    'santana row': 'Santana Row'
}

Days = {
    'monday': 'Monday',
    'tuesday': 'Tuesday',
    'wednesday': 'Wednesday',
    'thursday': 'Thursday',
    'friday': 'Friday',
    'saturday': 'Saturday',
    'sunday': 'Sunday'
}


congestionDFColumns = ['Source', 'Destination', 'Day', 'Response']
#congestionDF = pandas.DataFrame(columns=congestionDFColumns)

app = Flask(__name__)


@app.route('/initialize', methods=['GET'])
def initialize():
    global congestionDF
    for i in range(0, len(days)):
        y = pandas.Series(data=['Casa Verde Street', 'San Jose State University', days[i], id_3[i]],
                          index=congestionDFColumns)
        # x=pandas.DataFrame(data=['Casa Verde Street','San Jose State University',days[i],id_3[i]],columns=cols)
        congestionDF = congestionDF.append(y, ignore_index=True)
        print congestionDF
    for i in range(0, 2):
        y = pandas.Series(data=['Alum Rock Park', 'San Jose State University', days[i], id_2[i]],
                          index=congestionDFColumns)
        congestionDF = congestionDF.append(y, ignore_index=True)
        print congestionDF
        response = app.response_class(response="Cache Initialized", status=200)
        return response


def available(Source, Destination, Day, Type, Time):
    print "Checking Cache"
    if Type == 1:
        y = congestionDF[(congestionDF['Source'] == Source) &
                         (congestionDF['Destination'] == Destination) &
                         (congestionDF['Day'] == Day)]
        if y.shape[0] == 1:
            return list(y['Response'])
        else:
            return -1
    if Type == 2:
        print singleCongestionDF
        tempDF = singleCongestionDF[(singleCongestionDF['Source'] == Source) &
                                    (singleCongestionDF['Destination'] == Destination) &
                                    (singleCongestionDF['Day'] == Day) &
                                    (singleCongestionDF['Time'] == Time)]
        print tempDF
        if tempDF.shape[0] == 1:
            print tempDF['Response']
            return (list(tempDF['Response'])[0])
        else:
            return -1


@app.route('/getCongestion', methods=['POST'])
def getCongestion():
    data = json.loads(request.data)
    print type(data)
    data['Source'] = Places[data['Source'].lower()]
    data['Destination'] = Places[data['Destination'].lower()]
    data['Day']=Days[data['Day'].lower()]
    print "Checking Cache"
    storedCache=available(data['Source'],data['Destination'],data['Day'],2,data['Time'])
    print "Stored:",storedCache
    if storedCache != -1:
        response = app.response_class(response=json.dumps(storedCache), status=200, mimetype='application/json')
        return (response)
    else:
        command = '/home/student/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --packages ' \
                  'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pysparkArima.py ' + "\'" + \
                  data['Source'] + "\' \'" + data["Destination"] + "\' \'" + data["Time"] + "\' \'" + data["Day"] + "\'"
        commands.getoutput(command)
        try:
            with open('Congestionresult.json', 'r') as f:
                response = f.readlines()
            f.close()
            os.remove('Congestionresult.json')
            print response
            response = ast.literal_eval(response[0])
            response = list(response)
            print response
            response = app.response_class(response=json.dumps(response), status=200, mimetype='application/json')
            return (response)
        except:
            response = "<h>404 Not Found</h>"
            response = app.response_class(response=response, status=404)
            return (response)


@app.route('/getCongestionVsTime', methods=['POST'])
def getCongestionVsTime():
    data = json.loads(request.data)
    print data
    try:
        data["Source"] = Places[data["Source"].lower().strip()]
        print "Source:", data["Source"]
        data["Destination"] = Places[data["Destination"].lower().strip()]
        print "Destination:", data["Destination"]
        data["Day"] = Days[data["Day"].lower().strip()]
        print "Day:", data["Day"]
        response = available(data["Source"], data["Destination"], data["Day"],1,1)
        if (response != -1):
            print "Cache HIT!"
            print response
            response = app.response_class(response=json.dumps(response[0]), status=200, mimetype='application/json')
            return (response)
    except:
        response = "<h>Invalid POST Request</h>"
        response = app.response_class(response=response, status=404)
        return (response)

    command = '/home/student/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 travelTimevsTime.py ' + "\'" + \
              data["Source"] + "\' \'" + data["Destination"] + "\' \'" + data["Day"] + "\'" + " \'1\'"
    print command
    commands.getoutput(command)
    try:
        with open("get24HourCongestion.txt", "r") as f:
            response = f.readlines()
        f.close()
        os.remove("get24HourCongestion.txt")
        response = ast.literal_eval(response[0])
        response = list(response)
        response = app.response_class(response=json.dumps(response), status=200, mimetype='application/json')
        return (response)
    except:
        response = "<h>404 Not Found</h>"
        response = app.response_class(response=response, status=404)
        return (response)


@app.route('/getTravelTimeVsTime', methods=['POST'])
def getTravelTimeVsTime():
    data = json.loads(request.data)
    data["Source"] = Places[data["Source"].lower()]
    print "Source:", data["Source"]
    data["Destination"] = Places[data["Destination"].lower()]
    print "Destination:", data["Destination"]
    data["Day"] = Days[data["Day"].lower()]
    print "Day:", data["Day"]
    command = '/home/student/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 travelTimevsTime.py ' + "\'" + \
              data["Source"] + "\' \'" + data["Destination"] + "\' \'" + data["Day"] + "\'" + " \'2\'"
    print command
    commands.getoutput(command)
    try:
        with open("get24HourTravelTime.txt", "r") as f:
            response = f.readlines()
        os.remove("get24HourTravelTime.txt")
        response = ast.literal_eval(response[0])
        response = sorted(response, key=lambda k: k['label'])
        print response
        response = app.response_class(response=json.dumps(response), status=200, mimetype='application/json')
        return (response)
    except:
        response = "<h>404 Not Found<\h>"
        response = app.response_class(response=response, status=404)
        return (response)


if __name__ == "__main__":
    congestionDF = pandas.DataFrame(columns=congestionDFColumns)
    app.run()
