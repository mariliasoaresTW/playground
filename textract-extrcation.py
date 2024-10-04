import boto3
from botocore.exceptions import ClientError
import time
import json

session = boto3.Session(profile_name="dev")
textract_client = session.client('textract')

# S3 bucket and document details
bucket_name = 'redbridge-analysis-bucket'
document_name = 'Wells Fargo IC Apr 24.pdf'
output_file = 'Complete_textract_response.json'

try:
    # Start the Textract analysis for TABLES and FORMS
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

        if response['JobStatus'] in ['SUCCEEDED', 'FAILED']:
            break
        print("Job in progress...")
        time.sleep(5)

    if response['JobStatus'] == 'SUCCEEDED':
        all_blocks = []
        next_token = None

        while True:
            if next_token:
                response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
            else:
                response = textract_client.get_document_analysis(JobId=job_id)

            all_blocks.extend(response['Blocks'])

            if 'NextToken' in response:
                next_token = response['NextToken']
            else:
                break

        with open(output_file, 'w') as json_file:
            json.dump({'Blocks': all_blocks}, json_file, indent=4)

        print(f"Full response written to {output_file}")

except ClientError as e:
    print(f"An error occurred: {e}")
