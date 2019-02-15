import boto3
from botocore.exceptions import ClientError

session = boto3.Session(
    aws_access_key_id = 'mock',
    aws_secret_access_key = 'mock'
)

host = 'irl-0ifms-dev-cpo-bse06.inventale.com'
date = '2018-12-13'
db = 'rick_cpopro'
bucket = 'ifms-db-backups'

s3 = boto3.resource('s3')

client = boto3.client('s3')
# prefix = '{}/{}/{}'.format(host, date, db)
prefix = 'ams-ifms-prd-dat-msq01.inventale.com/2018-12-12/irving_cpopro'
dump_list = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

key = dump_list.get('Contents')[0].get('Key')
print(key)

try:
    s3.Bucket(bucket).download_file(key, 'rick_aws')
except ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

# a = s3.Storage('https://s3.console.aws.amazon.com/s3/buckets/ifms-db-backups')
print('ok')
