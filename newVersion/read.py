import os
import ast
with open('Congestionresult.json', 'r') as f:
     response = f.readlines()
     f.close()
os.remove('Congestionresult.json')
print response
response = ast.literal_eval(response[0])
response=list(response)
print response
response = app.response_class(response=json.dumps(response), status=200, mimetype='application/json')
print response
