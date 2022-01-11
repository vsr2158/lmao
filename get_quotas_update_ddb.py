import boto3
import io
import pandas as pd
from boto3.dynamodb.conditions import Key
import pprint

S3_BUCKET = '432177588334-quotas'
AWS_REGION = 'us-east-1'
QUOTAS_FILE = 'quotas.csv'
DDB_TABLE = 'accounts'

s3_resource = boto3.resource('s3', region_name = AWS_REGION)
s3_object = s3_resource.Object(S3_BUCKET,QUOTAS_FILE)
ddb_resource = boto3.resource('dynamodb', region_name = AWS_REGION)
ddb_table = ddb_resource.Table(DDB_TABLE)
dynamodb = boto3.resource('dynamodb',region_name = AWS_REGION )
table = dynamodb.Table('accounts')

def extract_accountId_for_parentId(OUId):
    ##Lets Query DDB TGable using GSI for a given OU
    ddb_item = ddb_table.query(
        IndexName='parentId-index',
        KeyConditionExpression=Key('parentId').eq(OUId)
    )
    #pprint.pprint(ddb_item)
    items = ddb_item['Items']
    print(len(items))
    accountIds = []
    for i in items:
        accountId = i['Id']
        accountIds.append(accountId)
    return (accountIds)

def read_quotas_csv():

    with io.BytesIO(s3_object.get()['Body'].read()) as f:
        df = pd.read_csv(f)
        return (df)
# once all the dataframe is constructed from CSV lets iterate over the rows

def update_ddb_with_quota(accountIds,QuotaCode,Region,DesiredQuotaValue,DesiredSupportLevel):
    for a in accountIds:
        #Lets get latest Item
        ddb_item = table.get_item(Key={'Id': a})

        #Update Item
#        ddb_item['Item']['Quotas'] = { QuotaCode:  { 'Regions' : {'Region': Region , 'DesiredQuotaValue': DesiredQuotaValue }}}

        ddb_item['Item']['Quotas'].update({ QuotaCode:  { 'Regions' : {'Region': Region , 'DesiredQuotaValue': DesiredQuotaValue }}})

        pprint.pprint(ddb_item)

        #Overwrite Item, this approach is not ideal instead we should update the existing Item
        table.put_item(Item = ddb_item['Item'])


df = read_quotas_csv()
for index, row in df.iterrows():
    print(row)
    OUId = row['OUId']
    QuotaCode = row['QuotaCode']
    DesiredQuotaValue = row['DesiredQuotaValue']
    DesiredSupportLevel = row['DesiredSupportLevel']
    Region = row['Region']
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print ('Iterating for OU : ', OUId, 'QuotaCode : ', QuotaCode, 'and Region : ', Region)
    accountIds = extract_accountId_for_parentId(OUId)
    print(accountIds)
    update_ddb_with_quota(accountIds,QuotaCode,Region,DesiredQuotaValue,DesiredSupportLevel)



