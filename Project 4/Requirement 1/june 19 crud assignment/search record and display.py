import json
import boto3

def lambda_handler(event, context):
    body = event['body']
    search_criteria = json.loads(body)
    print(search_criteria)
    val = search_criteria['search_value']
    sf = search_criteria['search_field']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('sim_student') 
    
    
    
    if search_criteria['search_field'] == 'rn':
        response = table.get_item(Key={'roll_no': val})
        if 'Item' in response:
            acc_data = response['Item']
            print(acc_data)
            mapped_data = [acc_data]
    elif sf == 'fname' :
        filter_expression = 'first_name = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
    elif sf == 'lname' :
        filter_expression = 'last_name = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
    elif sf == 'course' :
        filter_expression = 'course = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
    elif sf == 'isg' :
        filter_expression = 'is_grduate = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
        
    elif sf == 'Fees' :
        filter_expression = 'fees = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
        
    elif sf == 'doj' :
        filter_expression = 'date_of_joining = :acc_name'
        expression_attribute_values = {':acc_name': val}
        response = table.scan(FilterExpression=filter_expression,
                          ExpressionAttributeValues=expression_attribute_values)
        acc_data = response['Items']
        print(acc_data)
        mapped_data = acc_data
        print(mapped_data)
        
    return {
        'statusCode': 200,
        'body': json.dumps(mapped_data),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

    }
    }