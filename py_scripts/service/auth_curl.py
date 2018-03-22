import subprocess
import json


curl = 'curl -H "Content-Type: application/json" http://irl-ppt-mst-01.inventale.com:8888/v3/facts/?query=%5B%22and%22%2C%5B%22or%22%2C++%5B%22%3D%22%2C+%22name%22%2C+%22load_1m%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22ifms_packages%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22memoryfree_mb%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22uptime_days%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22sync_packages%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22memorysize_mb%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22ifmsjs_packages%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22hdd_list%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22processorcount%22%5D%2C++%5B%22%3D%22%2C+%22name%22%2C+%22ipaddress%22%5D%5D%2C+%5B%22%3D%22%2C+%22certname%22%2C+%22eu-smr-dev-01.inventale.com%22%5D%5D'

result = subprocess.check_output(curl, shell=True)

versions = json.loads(result.decode('utf8'))
print(versions)
