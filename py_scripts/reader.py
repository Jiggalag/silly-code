import json

def get_forecast(filename):
    with open(f'/home/polter/{filename}', 'r') as f:
        return json.loads(f.read())

result = list()

for item in ['8105.', '1363', '757', '8237', '801', '757n801']:
    result.append(get_forecast(item))

print('ok')
