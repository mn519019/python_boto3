import boto3
import os
import json


# export master_account_id="210084877764"
# export aggregator_name="aws-controltower-ConfigAggregatorForOrganizations"
# export account_ids='210062269947,042549984544,316371540355,266972066214'

# Vairables
MASTER = os.environ.get('master_account_id')
AGGREGATOR_NAME = os.environ.get('aggregator_name')
ACCOUNTS = os.environ.get('account_ids')

# Get current region function
def detect_current_region():
    global region
    region = os.environ.get('AWS_REGION')
    return region

# Get current account ID function 
def get_account_id():
    client = boto3.client("sts")
    res = client.get_caller_identity()["Account"]
    region = detect_current_region()

    if region == "ca-central-1" and res == MASTER:
        return res
    else:
        print("Wrong region is defined. Please make sure that correct region to be used.")

# def get_config_info():
#     client = boto3.client('config')
#     response = client.describe_configuration_aggregators()

#     aggregator_names = [aggregator['ConfigurationAggregatorName'] for aggregator in response['ConfigurationAggregators']]
#     return aggregator_names

def run_config_query(ACCOUNTS):
    # Create a Config client
    client = boto3.client('config')

    if ACCOUNTS:
        ACCOUNTS = ACCOUNTS.split(',')
    account_ids_str = ','.join(f"'{account_id.strip()}'" for account_id in ACCOUNTS)
    print(account_ids_str)
    # Define the query expression
    expression = f"""
    SELECT
      resourceType,
      COUNT(*)
    WHERE
      (
        accountId IN ({account_ids_str})
      )
    GROUP BY
      resourceType
    ORDER BY
      COUNT(*) DESC
    """

    # Run the query with an aggregator
    response = client.select_aggregate_resource_config(
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Expression=expression
    )

    # Pretty-print the results
    for result in response['Results']:
        pretty_result = json.loads(result)
        # Add the "AWS" column manually
        pretty_result['AWS'] = 'AWS'
        print(json.dumps(pretty_result, indent=4))


# Call the function with the account_ids variable
run_config_query(ACCOUNTS)
