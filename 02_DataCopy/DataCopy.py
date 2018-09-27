# DataCopy
# mjlee

import boto3
from pprint import pprint

# aws_access_key_id, aws_secret_access_key는 할당 받은 키로 사용
access_key = ""
secret_key = ""

vSrcProfileName = 'l-ellotte-dev'
vTgtProfileName = 'l-ellotte-tst'
vSrcRegionName = 'ap-northeast-2'
vTgtRegionName = 'ap-northeast-2'
#이관 대상은 아래의 list에 추가해서 수행

session = boto3.Session(profile_name=vSrcProfileName, region_name=vSrcRegionName)
dynamodb = session.client('dynamodb', region_name=vSrcRegionName)

session2 = boto3.Session(profile_name=vTgtProfileName, region_name=vTgtRegionName)
dynamodb2 = session2.client('dynamodb', region_name=vTgtRegionName)

#전체 테이블을 대상으로 할 경우 아래의 주석 해제
#vTableNameList = dynamodb.list_tables()['TableNames']
vTableNameList = ["PR_EVNT_PTCP"]
for vTableName in vTableNameList:
    try:
        IsTabCreated = dynamodb2.describe_table(TableName=vTableName)
        pass
    except Exception as ex:
        pprint(vTableName + " is not exists...")
        continue

    vData = dynamodb.scan(TableName = vTableName)
    vCount = vData['Count']
    items = []
    i = 0
    for line in vData['Items']:
        items.append({'PutRequest' : { 'Item' : line }})

        if len(items) == 25:
            dynamodb2.batch_write_item(RequestItems={vTableName: items})
            pprint(vTableName + " :: " + str(len(items)))
            items = []

        i = i + 1

    if len(items) > 0:
        dynamodb2.batch_write_item(RequestItems={vTableName: items})
        pprint(vTableName + " :: " + str(len(items)) + "/" + str(i))
    else:
        pprint(vTableName + " :: " + str(len(items)) + "/" + str(i))
