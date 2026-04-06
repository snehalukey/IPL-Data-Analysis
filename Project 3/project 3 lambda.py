import json
import boto3
import os
from decimal import Decimal
from datetime import datetime, date
import csv
from io import StringIO



def lambda_handler(event, context):

    current_date = date.today()
    s3 = boto3.client('s3')
    
    print(event)
    # Retrieve the message from the event
    sqs_message = json.loads(event['Records'][0]['body'])
    
    # Extract subject and body from the SQS message
    sid = sqs_message['tn_rf_id']
    vc = sqs_message['Txn_no']
    tdate = sqs_message['tn_dt']
    ps = sqs_message['amt']
    gst = sqs_message['gst']
    ed = sqs_message['excise_duty']
    

    d1 = {"txn_id" : int(os.environ['counter']), 
        "voucher_code" : vc,
          "txn_type" : "C",
          "txn_date" : tdate,
          "acc_name" : "Product Sales",
          "txn_amt"  : ps ,
          "source_system_id" : "1",
          "source_system_txn_id" : sid}
    
    # df1 = pd.DataFrame(d1)
    
    d2 = {"txn_id" :int(os.environ['counter']) + 1,
        "voucher_code" : vc,
          "txn_type" : "C",
          "txn_date" : tdate,
          "acc_name" : "Goods and Service Tax",
          "txn_amt"  : gst ,
          "source_system_id" : "1",
          "source_system_txn_id" : sid}
    
    # df2 = pd.DataFrame(d2)
    
    d3 = {"txn_id" : int(os.environ['counter']) +2, 
        "voucher_code" : vc,
          "txn_type" : "C",
          "txn_date" : tdate,
          "acc_name" : "Excise Duty",
          "txn_amt"  : ed ,
          "source_system_id" : "1",
          "source_system_txn_id" : sid}


    dynamodb = boto3.client('dynamodb')
    response = dynamodb.scan(TableName='acct_master_g4')
    items = response['Items']
    
    for x in items :
        if x['acc_name']['S'] == "Product Sales":
            d1['acc_no'] = x['acc_no']['S']
        elif x['acc_name']['S'] == "Goods and Service Tax":
            d2['acc_no'] = x['acc_no']['S']
        elif x['acc_name']['S'] == "Excise Duty":
            d3['acc_no'] = x['acc_no']['S']
    
    
    
    dynamodb = boto3.resource('dynamodb')
    table =dynamodb.Table("ledger_txn_g4")
    
    items_to_add = [d1, d2, d3]
    with table.batch_writer() as batch:
        for y in items_to_add:
            a = datetime.strptime(y["txn_date"], "%Y-%m-%d %H:%M %p").date()
            if a <= current_date:
                res = batch.put_item(Item={
                    'txn_id' : str(y['txn_id']),
                    'voucher_code': y['voucher_code'],
                    'txn_type': y['txn_type'],
                    "txn_date" : y["txn_date"],
                    'acc_no': y['acc_no'],
                    'txn_amt': Decimal(str(y['txn_amt'])),
                    'source_system_id': y['source_system_id'],
                    'source_system_txn_id': y['source_system_txn_id']
            })
            else:
                csv_data = StringIO()
                csv_writer = csv.writer(csv_data)
                csv_writer.writerow(y.keys())  # Write header
                csv_writer.writerow(y.values())  # Write values
                csv_content = csv_data.getvalue()
                
                s3.put_object(Body=csv_content, Bucket="g4-project2", Key="failed_records")
                

    
    
    counter = int(os.environ.get('counter', 0)) + 3
    os.environ['counter'] = str(counter)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Record updated!')
    }
