import json
import boto3

def lambda_handler(event, context):
    
    body = event['body']
    search_criteria = json.loads(body)
    account_no = search_criteria.get('acc_no')
    account_name = search_criteria.get('acc_name')
    account_type = search_criteria.get('acc_type')
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('acct_master_g4')  
    
    if account_no:
        response = table.get_item(Key={'acc_no': account_no})
        
        print(response)
        if 'Item' in response:
            acc_data = response['Item']
            print(acc_data)
            mapped_data = [acc_data]
            print(mapped_data)
        
    elif account_name and not account_no : 
        filter_expression = 'acc_name = :acc_name'
        expression_attribute_values = {':acc_name': account_name}

    # Perform the scan operation with the filter expression
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
    
    # Retrieve the scanned items
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
        
    elif account_type and not account_no and not account_name : 
        filter_expression = 'acc_type = :acc_type'
        expression_attribute_values = {':acc_type': account_type}
    
    # Perform the scan operation with the filter expression
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
    
    # Retrieve the scanned items
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
    
        


    return {
        'statusCode': 200,
        'body': json.dumps(mapped_data),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

    }
    }
