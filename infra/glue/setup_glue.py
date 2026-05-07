import boto3
import time

iam = boto3.client('iam', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

account_id = '561764228129'
role_name = 'AWSGlueServiceRole-GlobalPartners'
bucket = 'global-partners-data-lake-561764228129'

# 1. Create IAM Role for Glue
assume_role_policy = '''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'''

try:
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy
    )
    print(f"Created role {role_name}")
except iam.exceptions.EntityAlreadyExistsException:
    print(f"Role {role_name} already exists")

iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
)
iam.put_role_policy(
    RoleName=role_name,
    PolicyName='S3AccessPolicy',
    PolicyDocument='''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::global-partners-data-lake-561764228129",
        "arn:aws:s3:::global-partners-data-lake-561764228129/*"
      ]
    }
  ]
}'''
)

print("Attached policies to role")
time.sleep(5)  # Wait for IAM propagation

role_arn = f'arn:aws:iam::{account_id}:role/{role_name}'

# 2. Define Jobs
jobs = [
    {
        "name": "bronze_to_silver_order_items",
        "script": f"s3://{bucket}/scripts/bronze_to_silver/to_silver_order_items.py"
    },
    {
        "name": "silver_to_gold_dimensions",
        "script": f"s3://{bucket}/scripts/silver_to_gold/to_gold_dimensions.py"
    },
    {
        "name": "silver_to_gold_order_summary",
        "script": f"s3://{bucket}/scripts/silver_to_gold/to_gold_order_summary.py"
    },
    {
        "name": "silver_to_gold_daily_sales",
        "script": f"s3://{bucket}/scripts/silver_to_gold/to_gold_daily_sales.py"
    },
    {
        "name": "silver_to_gold_customer_rfm",
        "script": f"s3://{bucket}/scripts/silver_to_gold/to_gold_customer_rfm.py"
    }
]

for job in jobs:
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
                '--S3_BUCKET': bucket
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

# 3. Create Workflow
workflow_name = 'GlobalPartners-Medallion-Pipeline'
try:
    glue.create_workflow(Name=workflow_name, Description='Daily Medallion Pipeline')
    print(f"Created workflow {workflow_name}")
except glue.exceptions.AlreadyExistsException:
    print(f"Workflow {workflow_name} already exists")

# 4. Create Triggers for Workflow
print("Creating triggers...")
try:
    # Trigger 1: Schedule to start the workflow and run Bronze -> Silver
    glue.create_trigger(
        Name='Start-Pipeline',
        WorkflowName=workflow_name,
        Type='SCHEDULED',
        Schedule='cron(0 0 * * ? *)',
        Actions=[{'JobName': 'bronze_to_silver_order_items'}]
    )
    
    # Trigger 2: After Bronze -> Silver, run Silver -> Gold Dimensions
    glue.create_trigger(
        Name='Trigger-Silver-to-Gold-Dims',
        WorkflowName=workflow_name,
        Type='CONDITIONAL',
        Predicate={
            'Logical': 'ANY',
            'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': 'bronze_to_silver_order_items', 'State': 'SUCCEEDED'}]
        },
        Actions=[{'JobName': 'silver_to_gold_dimensions'}]
    )
    
    # Trigger 3: After Dimensions, run facts
    glue.create_trigger(
        Name='Trigger-Silver-to-Gold-Facts',
        WorkflowName=workflow_name,
        Type='CONDITIONAL',
        Predicate={
            'Logical': 'ANY',
            'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': 'silver_to_gold_dimensions', 'State': 'SUCCEEDED'}]
        },
        Actions=[
            {'JobName': 'silver_to_gold_order_summary'},
            {'JobName': 'silver_to_gold_daily_sales'},
            {'JobName': 'silver_to_gold_customer_rfm'}
        ]
    )
    print("Triggers created.")
except Exception as e:
    print(f"Trigger creation skipped/failed: {e}")

print("Glue setup complete.")
