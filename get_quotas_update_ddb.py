import boto3
import io
import pandas as pd
from boto3.dynamodb.conditions import Key
import pprint

S3_BUCKET = '432177588334-quotas'
AWS_REGION = 'us-east-1'
QUOTAS_FILE = 'quotas.csv'
DDB_TABLE = 'account-raovi'

s3_resource = boto3.resource('s3', region_name = AWS_REGION)
s3_object = s3_resource.Object(S3_BUCKET,QUOTAS_FILE)
ddb_resource = boto3.resource('dynamodb', region_name = AWS_REGION)
ddb_table = ddb_resource.Table(DDB_TABLE)
org = boto3.client('organizations')

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

def get_child_of_ou(OUId):
    children_dict = org.list_children(
        ParentId = OUId,
        ChildType = 'ACCOUNT',
    )
    #Lets check if we got next token in response to iterate again
    if 'NextToken' in children_dict:
        print('NextToken Found')
        ## this needs to be addressed
    else:
        print('NextToken Not Found')
    # Convert children_dict to children_list
    children_list = children_dict['Children']
    account_id_list = []
    for c in children_list:
        account_id_list.append(c['Id'])
    return(account_id_list)

def read_quotas_csv():

    with io.BytesIO(s3_object.get()['Body'].read()) as f:
        df = pd.read_csv(f)
        return (df)
# once all the dataframe is constructed from CSV lets iterate over the rows

def update_ddb_with_quota(ddb_key,ddb_item):
    #Lets get latest Item
    #ddb_item = table.get_item(Key={'Id': a})
    #ddb_item['Item']['Quotas'].update({ ServiceCode : {QuotaCode:  {'Regions': { Region : { 'DesiredQuotaValue': DesiredQuotaValue, 'Status': 'New'}}}}})
    #pprint.pprint(ddb_item)

    #Overwrite Item, this approach is not ideal instead we should update the existing Item
    print(ddb_item)
    print(ddb_item['OUId'])
    ddb_table.update_item(Key = ddb_key,
        UpdateExpression="SET OUId = :OUId, DesiredQuotaValue = :DesiredQuotaValue, Staate = :Staate",
        ExpressionAttributeValues={':OUId': OUId, ':DesiredQuotaValue': DesiredQuotaValue, ':Staate': Staate})

df = read_quotas_csv()
for index, row in df.iterrows():
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print(row)
    OUId = row['OUId']
    QuotaCode = row['QuotaCode']
    ServiceCode = row['ServiceCode']
    DesiredQuotaValue = row['DesiredValue']
    DesiredSupportLevel = row['DesiredSupportLevel']
    Region = row['Region']
    Staate = 'NEW'
    print ('Iterating for OU : ', OUId,'ServiceCode :', ServiceCode, 'QuotaCode : ', QuotaCode, 'and Region : ', Region)
    #accountIds = extract_accountId_for_parentId(OUId)
    account_id_list = get_child_of_ou(OUId)
    print(account_id_list)
    ddb_sort_key = Region+'_'+ServiceCode+'_'+QuotaCode
    for a in account_id_list:
        print(a, ddb_sort_key)
        ddb_key = {'account_id' :a, 'region_sc_qc': ddb_sort_key}
        ddb_item = {'OUId': OUId, 'DesiredQuotaValue': DesiredQuotaValue, 'Staate': Staate}
        update_ddb_with_quota(ddb_key,ddb_item)



