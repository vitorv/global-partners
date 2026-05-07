import boto3

glue = boto3.client('glue', region_name='us-east-1')

# 1. Remove customer_rfm from the parallel trigger
try:
    glue.update_trigger(
        Name='Trigger-Silver-to-Gold-Facts',
        TriggerUpdate={
            'Predicate': {
                'Logical': 'ANY',
                'Conditions': [
                    {'LogicalOperator': 'EQUALS', 'JobName': 'silver_to_gold_dimensions', 'State': 'SUCCEEDED'}
                ]
            },
            'Actions': [
                {'JobName': 'silver_to_gold_order_summary'},
                {'JobName': 'silver_to_gold_daily_sales'}
            ]
        }
    )
    print("Updated Trigger-Silver-to-Gold-Facts")
except Exception as e:
    print(f"Error updating Trigger-Silver-to-Gold-Facts: {e}")

# 2. Create the dependent trigger for customer_rfm
try:
    glue.create_trigger(
        Name='Trigger-Gold-to-RFM',
        WorkflowName='GlobalPartners-Medallion-Pipeline',
        Type='CONDITIONAL',
        Predicate={
            'Logical': 'ANY',
            'Conditions': [
                {'LogicalOperator': 'EQUALS', 'JobName': 'silver_to_gold_order_summary', 'State': 'SUCCEEDED'}
            ]
        },
        Actions=[
            {'JobName': 'silver_to_gold_customer_rfm'}
        ]
    )
    print("Created Trigger-Gold-to-RFM")
except glue.exceptions.AlreadyExistsException:
    print("Trigger-Gold-to-RFM already exists")
except Exception as e:
    print(f"Error creating Trigger-Gold-to-RFM: {e}")

# 3. Activate the new trigger
try:
    glue.start_trigger(Name='Trigger-Gold-to-RFM')
    print("Activated Trigger-Gold-to-RFM")
except Exception as e:
    print(f"Error activating trigger: {e}")
