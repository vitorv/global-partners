import boto3

glue = boto3.client('glue', region_name='us-east-1')
bucket = 'global-partners-data-lake-561764228129'

job_names = [
    "bronze_to_silver_order_items",
    "silver_to_gold_dimensions",
    "silver_to_gold_order_summary",
    "silver_to_gold_daily_sales",
    "silver_to_gold_customer_rfm"
]

for job_name in job_names:
    response = glue.get_job(JobName=job_name)
    job = response['Job']
    
    # Get existing arguments and add extra-py-files
    default_args = job.get('DefaultArguments', {})
    default_args['--extra-py-files'] = f"s3://{bucket}/scripts/config.py"
    
    # Update the job
    glue.update_job(
        JobName=job_name,
        JobUpdate={
            'Role': job['Role'],
            'ExecutionProperty': job['ExecutionProperty'],
            'Command': job['Command'],
            'DefaultArguments': default_args,
            'MaxRetries': job['MaxRetries'],
            'Timeout': job['Timeout'],
            'WorkerType': job['WorkerType'],
            'NumberOfWorkers': job['NumberOfWorkers'],
            'GlueVersion': job['GlueVersion']
        }
    )
    print(f"Updated job {job_name} with extra-py-files")
