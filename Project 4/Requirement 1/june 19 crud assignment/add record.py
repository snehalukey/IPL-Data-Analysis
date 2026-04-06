import json
import boto3

def lambda_handler(event, context):
    body = event['body']
    search_criteria = json.loads(body)
    print(search_criteria)
    rollno = search_criteria[0].get('value')
    print(rollno)
    fname = search_criteria[1].get('value')
    print(fname)
    lname = search_criteria[2].get('value')
    course = search_criteria[3].get('value')
    doj = search_criteria[4].get('value')
    fees = search_criteria[5].get('value')
    isg = search_criteria[6].get('value')

    session = boto3.Session()
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('sim_student')  
    response = table.get_item(Key={'roll_no': rollno})
    if 'Item' not in response:
        table.put_item(
            Item={
                'roll_no' : rollno,
                'first_name': fname,
                'last_name' : lname,
                'course' : course,
                'date_of_joining' : doj,
                'fees': fees,
                'is_grduate' : isg
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
