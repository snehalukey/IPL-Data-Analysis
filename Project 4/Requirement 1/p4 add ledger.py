import json
import boto3

def lambda_handler(event, context):
    body = event['body']
    search_criteria = json.loads(body)
    account_no = search_criteria.get('acc_no')
    account_name = search_criteria.get('acc_name')
    account_type = search_criteria.get('acc_type')
    account_desc = search_criteria.get('acc_desc')
    
    session = boto3.Session()
    dynamodb = session.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('acct_master_g4')  
    response = table.get_item(Key={'acc_no': account_no})
    if 'Item' not in response:
        table.put_item(
            Item={
                'acc_no' : account_no,
                'acc_name': account_name,
                'acc_type' : account_type,
                'acc_desc' : account_desc
            }
        )
    else:
        print("Record already exists!")
    
    return {
        'statusCode': 200,
        'body': "hello",
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'
        }
    }
