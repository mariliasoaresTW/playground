import boto3
from botocore.exceptions import ClientError
import time
import json
import csv
# AWS Textract client
session = boto3.Session(profile_name= "dev")
textract_client = session.client('textract')

# S3 bucket and document details
bucket_name = 'redbridge-analysis-bucket'
document_name = 'Wells Fargo IC Apr 24.pdf'
output_file = 'textract_response.json'


try:
    response = textract_client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': document_name
            }
        },
        FeatureTypes=['TABLES', 'FORMS']
    )

    job_id = response['JobId']
    print(f"Job started with ID: {job_id}")


    while True:
        response = textract_client.get_document_analysis(JobId=job_id)
        # Check if the job is complete
        if response['JobStatus'] in ['SUCCEEDED', 'FAILED']:
            break

        print("Job in progress...")
        time.sleep(5)

    if response['JobStatus'] == 'SUCCEEDED':
        # print(json.dumps(response, indent=4))
        with open(output_file,'w') as json_file:
            json.dump(response,json_file, indent=4)
        print(f"Response written to {output_file}")

except ClientError as e:
    print(f"An error occurred: {e}")