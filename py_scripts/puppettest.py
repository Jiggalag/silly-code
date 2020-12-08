import requests

pdb = requests.get("http://inv-prd-ppt-01.inventale.com:8085/pdb/query/v4/facts?query=%s").text
uri = requests.get("http://irl-ppt-mst-01.inventale.com:8888/v3/facts/?query=%s").text
print('stop')
