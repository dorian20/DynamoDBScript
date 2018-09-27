# TableDrop
# mjlee

import boto3
from pprint import pprint

vTgtProfileName = 'l-b2-tst'
vTgtRegionName = 'ap-northeast-2'
#이관 대상은 아래의 list에 추가해서 수행

session = boto3.Session(profile_name=vTgtProfileName, region_name=vTgtRegionName)
dynamodb = session.client('dynamodb', region_name=vTgtRegionName)

#전체 테이블을 대상으로 할 경우 아래의 주석 해제
vTableNameList = dynamodb.list_tables()['TableNames']
#vTableNameList = [
#        "AURORA_SNS_LIST",
#        "B2_DYNAMO_TABLE_DESCRIBE",
#        "B2_METRIC_COLLECT_ITEM",
#        "B2_SNS_LIST",
#        "CC_BBS_EXT",
#        ]

for vTableName in vTableNameList:
    #위험하므로 주석 처리함. 사용할 땐 주석 해제 후..
    #dynamodb.delete_table(TableName=vTableName)