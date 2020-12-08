import boto3

query = "SELECT ad_id FROM movement LIMIT 5;"
database = "segmentsdb"
athena_result_bucket = "s3://iq-segments-automation-pavel/"
session = boto3.session.Session(aws_access_key_id='test', aws_secret_access_key='test')
client = session.client('athena')

response = client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={
        'Database': database
    },
    ResultConfiguration={
        'OutputLocation': athena_result_bucket,
    }
)

query_execution_id = response["QueryExecutionId"]