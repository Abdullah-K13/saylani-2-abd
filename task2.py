from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
import pandas as pd
import io

# Set your AWS Access Key and Secret Access Key
aws_access_key_id = 'asd'
aws_secret_access_key = 'asdasdasd'

# SQS configuration
sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/430118839940/airflow'  # Replace with your SQS Queue URL

def process_file(file_key, bucket_name):
    """
    This function processes the file, transforming it and uploading the transformed file back to S3.
    Modify this as per your transformation logic.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-east-1'
    )

    # Download the file from S3
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    raw_data = file_obj['Body'].read().decode('utf-8')

    # Load JSON into pandas dataframe
    try:
        data = json.loads(raw_data)
        df = pd.json_normalize(data)
        transformed_data = df.to_csv(index=False)
    except json.JSONDecodeError:
        print("Error decoding JSON.")
        return

    # Upload the transformed data to a new S3 location
    transformed_key = file_key.replace('raw/', 'transformed/')  # Example location change
    s3_client.put_object(Bucket=bucket_name, Key=transformed_key, Body=transformed_data)

    print(f"Transformed file uploaded to: {transformed_key}")

    
def wait_for_message_from_sqs():
    """
    This function listens to SQS messages using boto3 and retrieves the file key and bucket name for processing.
    """
    sqs_client = boto3.client(
        'sqs',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-east-1'
    )

    # Receive a message from SQS queue
    response = sqs_client.receive_message(
        QueueUrl=sqs_queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=10  # Long polling
    )

    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        
        # Log the message body for debugging
        print("Received SQS message body:", message['Body'])
        
        # Try to extract the file key and bucket name from the SQS message
        try:
            records = json.loads(message['Body'])['Records']
            file_key = records[0]['s3']['object']['key']
            bucket_name = records[0]['s3']['bucket']['name']
            print(f"File key extracted: {file_key}")
            print(f"Bucket name extracted: {bucket_name}")
            
            # Delete the message from the queue after processing it
            sqs_client.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=receipt_handle
            )
            return file_key, bucket_name
        except KeyError as e:
            # Handle case where expected keys are missing
            print(f"Error: {str(e)} - The expected structure is missing from the message: {message['Body']}")
            return None, None
    else:
        print("No messages found in the queue.")
        return None, None

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    's3_file_transformation_sqs_trigger',
    default_args=default_args,
    schedule_interval='@once',  # Set to @once to run manually or schedule as needed
    catchup=False,
) as dag:

    # Task to listen for messages from SQS and process them
    def on_message_found(**kwargs):
        file_key, bucket_name = wait_for_message_from_sqs()
        if file_key and bucket_name:
            process_file(file_key, bucket_name)

    # Task to process the file
    process_file_task = PythonOperator(
        task_id='process_file',
        python_callable=on_message_found,
        provide_context=True,
    )

    process_file_task
