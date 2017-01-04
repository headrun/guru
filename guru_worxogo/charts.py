import json
import urllib.request

body={"infile":"{xAxis: {categories: ['2016-07-09', '2016-07-10', '2016-07-11', '2016-07-12', '2016-07-13', '2016-07-14']},series: [{'data': [20.414855, 20.381449, 20.404855, 20.316957, 20.231377, 20.156594], 'name': 'UD'}, {'data': [21.431848, 21.450761, 21.505761, 21.417717, 21.328478, 21.230978], 'name': 'US'}, {'data': [1.963314, 1.959467, 1.959645, 1.945917, 1.93426, 1.919231], 'name': 'UW'}, {'data': [1.467207, 1.468045, 1.479609, 1.469609, 1.462179, 1.45352], 'name': 'UG'}, {'data': [5.186654, 5.175809, 5.180699, 5.156029, 5.158272, 5.136434], 'name': 'UK'}]};","constr":"Chart", "outfile":"chart_img.png"}
req= urllib.request.Request("http://localhost:3003")
req.add_header("Content-Type", "application/json; charset=utf-8")
jsondata= json.dumps(body)
jsondataasbytes= jsondata.encode('utf-8')
req.add_header("Content-Length", len(jsondataasbytes))
response= urllib.request.urlopen(req, jsondataasbytes)
print(response.read())
