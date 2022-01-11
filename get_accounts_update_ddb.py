import boto3
from botocore.config import Config
import json

DDB_TABLE = 'accounts'

my_config = Config(
    region_name = 'us-east-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

def load_data(l):
    print("loading in DDB " , l )
    dynamodb = boto3.resource('dynamodb', config=my_config)
    table = dynamodb.Table('accounts')
    table.put_item(Item=l)

org = boto3.client('organizations')
accounts = org.list_accounts()
#clean up response and get list of accounts
list_accounts = accounts["Accounts"]
number_of_accounts = len(list_accounts)
print("Number of accounts : ", number_of_accounts)
for l in list_accounts:
    print(l)
    accountId = l['Id']
    print(accountId)
    l.pop('JoinedTimestamp')
    #lets get the parent OU
    parent = org.list_parents(
        ChildId = accountId
    )
    print('+++++++++++')
    print(parent)
    print('+++++++++++')
    #if Parent is OU, lets add OU ID to the disctionary
    parentId = parent['Parents'][0]['Id']
    parentType = parent['Parents'][0]['Type']
    l['parentId'] = parentId
    l['parentType'] = parentType
    load_data(l)

