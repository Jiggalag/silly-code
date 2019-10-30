import py_scripts.helpers.cli_helper as cliH
import datetime
import time

date = datetime.date.today() - datetime.timedelta(days=1)

jobs = [
    "refresh",
    "SampleToCSV(-sDt {0} -eDt {0})".format(date),
    "SimulationToCSV(--current)"
]

host = 'dev01.inventale.com'
cliPath = 'cli'
rmiPort = 9047

cliConnection = cliH.CliConnection(cliPath, host, rmiPort)

for job in jobs:
    if not cliConnection.is_job_running():
        cliConnection.run_job(job)
    while cliConnection.is_job_running():
        time.sleep(30)
    if "Sample" in job:
        pass
        # TODO: copySampleToStorage
    elif "Simulation":
        pass
        # TODO: copySimultaionToStorage
    else:
        pass