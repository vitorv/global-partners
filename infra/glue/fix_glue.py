import boto3

glue = boto3.client('glue', region_name='us-east-1')

account_id = '561764228129'
role_arn = f'arn:aws:iam::{account_id}:role/AWSGlueServiceRole-GlobalPartners'
bucket = 'global-partners-data-lake-561764228129'
workflow_name = 'GlobalPartners-Medallion-Pipeline'

missing_jobs = [
    {
        "name": "bronze_to_silver_date_dim",
        "script": f"s3://{bucket}/scripts/bronze_to_silver/to_silver_date_dim.py"
    },
    {
        "name": "bronze_to_silver_order_item_options",
        "script": f"s3://{bucket}/scripts/bronze_to_silver/to_silver_order_item_options.py"
    }
]

for job in missing_jobs:
    try:
        glue.create_job(
            Name=job["name"],
            Role=role_arn,
            ExecutionProperty={'MaxConcurrentRuns': 1},
            Command={
                'Name': 'glueetl',
                'ScriptLocation': job["script"],
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--enable-continuous-cloudwatch-log': 'true',
                '--PIPELINE_ENV': 'aws',
                '--S3_BUCKET': bucket,
                '--extra-py-files': f"s3://{bucket}/scripts/config.py"
            },
            MaxRetries=0,
            Timeout=2880,
            WorkerType='G.1X',
            NumberOfWorkers=2,
            GlueVersion='4.0'
        )
        print(f"Created job {job['name']}")
    except glue.exceptions.AlreadyExistsException:
        print(f"Job {job['name']} already exists")

# Update Start-Pipeline Trigger
try:
    glue.update_trigger(
        Name='Start-Pipeline',
        TriggerUpdate={
            'Schedule': 'cron(0 0 * * ? *)',
            'Actions': [
                {'JobName': 'bronze_to_silver_order_items'},
                {'JobName': 'bronze_to_silver_date_dim'},
                {'JobName': 'bronze_to_silver_order_item_options'}
            ]
        }
    )
    print("Updated Start-Pipeline trigger")
except Exception as e:
    print(f"Error updating Start-Pipeline: {e}")

# Update Trigger-Silver-to-Gold-Dims to wait for all 3 bronze_to_silver jobs
try:
    glue.update_trigger(
        Name='Trigger-Silver-to-Gold-Dims',
        TriggerUpdate={
            'Predicate': {
                'Logical': 'AND',
                'Conditions': [
                    {'LogicalOperator': 'EQUALS', 'JobName': 'bronze_to_silver_order_items', 'State': 'SUCCEEDED'},
                    {'LogicalOperator': 'EQUALS', 'JobName': 'bronze_to_silver_date_dim', 'State': 'SUCCEEDED'},
                    {'LogicalOperator': 'EQUALS', 'JobName': 'bronze_to_silver_order_item_options', 'State': 'SUCCEEDED'}
                ]
            },
            'Actions': [{'JobName': 'silver_to_gold_dimensions'}]
        }
    )
    print("Updated Trigger-Silver-to-Gold-Dims")
except Exception as e:
    print(f"Error updating Trigger-Silver-to-Gold-Dims: {e}")
