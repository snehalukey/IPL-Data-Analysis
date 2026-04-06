#final code for project 2 lambda job

import json
import boto3

def lambda_handler(event, context):
    
    print(event)
    print(type(event))
    
    key = str(event['Records'][0])
    print(key)
    
    def start_job(job_name):
        glue_client = boto3.client('glue')

        try:
        # Start the Glue job
            response = glue_client.start_job_run(JobName=job_name)
        except:
            print("Couldnt run job")
            
        
            
    if '.parquet' in key:
        start_job("g4_p2_parquet")
    elif '.avro' in key:
        start_job("g4_p2_avro")

    

    return {
        'statusCode': 200,
        'body': json.dumps('Job Run Successfully')
    }
