import json
import boto3
import decimal

def lambda_handler(event, context):
    
    def strr(obj):
        return str(obj)
    
    body = event['body']
    search_criteria = json.loads(body)
    print(search_criteria)
    account_no = search_criteria.get('acc_no')
    # account_name = search_criteria.get('acc_name')
    # account_type = search_criteria.get('acc_type')
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ledger_txn_g4')
    
    filter_expression = 'voucher_code = :vc'
    expression_attribute_values = {':vc': account_no}

    # Perform the scan operation with the filter expression
    response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        
    acc_data = response['Items']
    print(acc_data)
    x = []
    for acc in acc_data:
        for key, value in acc.items():
            acc[key] = strr(value)
        mapped_data = {
            'txn_id': acc.get('txn_id', ''),
            'voucher_code': acc.get('voucher_code', ''),
            'txn_date': acc.get('txn_date', ''),
            'txn_type': acc.get('txn_type', ''),
            'acc_no': acc.get('acc_no', ''),
            'txn_amt': acc.get('txn_amt', ''),
            'source_system_id': acc.get('source_system_id', ''),
            'source_system_txn_id': acc.get('source_system_txn_id', '')
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
