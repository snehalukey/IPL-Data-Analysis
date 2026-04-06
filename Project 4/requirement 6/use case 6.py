import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    def strr(obj):
        return str(obj)
        
    def todate(obj):
        obj = datetime.strptime(obj, "%Y-%m-%d").date()
        obj = obj.isoformat()
        return obj
    
    body = event['body']
    search_criteria = json.loads(body)
    print(search_criteria)
    from_date = todate(search_criteria.get('from'))
    to_date = todate(search_criteria.get('to_date'))
    # account_name = search_criteria.get('acc_name')
    # account_type = search_criteria.get('acc_type')

    
    dynamodb_client = boto3.client('dynamodb')
    table_name = 'ledger_txn_g4'
    
    
    filter_expression = 'txn_date between :start_date and :end_date'

# Define the expression attribute values
    expression_attribute_values = {
        ':start_date': {'S': from_date},
        ':end_date': {'S': to_date}
    }
    # Perform the scan operation with the filter expression
    response = dynamodb_client.scan(
        TableName=table_name,
        FilterExpression=filter_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    
    # Get the items from the response
    # items = response['Items']
    
    # filter_expression = 'voucher_code = :vc'
    # expression_attribute_values = {':vc': account_no}

    # # Perform the scan operation with the filter expression
    # response = table.scan(FilterExpression=filter_expression,
    #                       ExpressionAttributeValues=expression_attribute_values)
        
    acc_data = response['Items']
    # print(acc_data)
    x = []
    for acc in acc_data:
        # for key, value in acc.items():
            # acc[key] = strr(value)
        mapped_data = {
            "txn_id" : acc.get('txn_id', {}).get('S', ''),
            'voucher_code': acc.get('voucher_code', {}).get('S', ''),
            'txn_date': acc.get('txn_date', {}).get('S', ''),
            'txn_type': acc.get('txn_type', {}).get('S', ''),
            'acc_no': acc.get('acc_no', {}).get('S', ''),
            'txn_amt': acc.get('txn_amt', {}).get('N', ''),
            'source_system_id': acc.get('source_system_id', {}).get('S', ''),
            'source_system_txn_id': acc.get('source_system_txn_id', {}).get('S', '')
        }
        x.append(mapped_data)

    return {
        'statusCode': 200,
        'body': json.dumps(x),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

    }
    }
