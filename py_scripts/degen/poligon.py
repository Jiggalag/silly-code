import pyodbc
import pandas.io.sql as psql

cnxn = pyodbc.connect(connection_info)
cursor = cnxn.cursor()
sql = "SELECT * FROM TABLE"

df = psql.frame_query(sql, cnxn)
cnxn.close()