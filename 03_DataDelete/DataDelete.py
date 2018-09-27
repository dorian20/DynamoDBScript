import boto3, sys
from botocore.exceptions import ClientError
from pprint import pprint

# aws_access_key_id, aws_secret_access_key는 할당 받은 키로 사용
access_key = ""
secret_key = ""

#한번에 몇건씩 지울 것인지 설정하는 파라미터
limitCount = 1000

#Table명
try:
    vTableName = sys.argv[1]
except IndexError:
    print ("No Input!!")
    print ("ex) python DataDelete.py [TableName]")

vYesNo = input("Do you want to delete all data in "+vTableName+"(Y/[N])?")

if vYesNo is None or (vYesNo != "Y" and vYesNo != "y") or vYesNo == "":
    print("Good Bye")
    exit(0)


dynamodb = boto3.resource(service_name='dynamodb',
                          aws_access_key_id = access_key,
                          aws_secret_access_key = secret_key,
                          region_name = 'ap-northeast-2')
try:
    table = dynamodb.Table(vTableName)
    vData = table.scan(Limit=limitCount)
except ClientError as ce:
    if ce.response['Error']['Code'] == 'ResourceNotFoundException':
        print("No Table. Check Again!")
    else:
        print(str(ce.response['Error']['Code']))
    exit(0)

vCount = vData['Count']

# 데이터가 없으므로 종료함
if vCount == 0:
    print('No data found.')
    exit(0)


# 데이터가 있으므로 수행함
vLastEvalKey = vData.pop('LastEvaluatedKey',None)
vKeySchema = table.key_schema

# 키 추가 로직
vKey1 = vKeySchema[0]['AttributeName']
vKey2 = None
if len(vKeySchema) == 2:
    vKey2 = vKeySchema[1]['AttributeName']

# 삭제용 function
def dataDelete(vData):
    for line in vData:
        vKey = {vKey1 : line[vKey1]}
        
        # 키가 2개일 때는 두번째 키를 추가
        if vKey2 is not None:
            vKey[vKey2] = line[vKey2]

        table.delete_item(Key=vKey)

        #pprint(result)
             

dataDelete(vData=vData['Items'])

while vLastEvalKey is not None:
    vData = table.scan(Limit=limitCount, ExclusiveStartKey=vLastEvalKey)
    dataDelete(vData=vData['Items'])
    vLastEvalKey = vData.pop('LastEvaluatedKey',None)
    
vData = table.scan(Limit=1)
vCount = vData['Count']

if vCount > 0:
    print('Data exists on table. Plz try again...')
    exit(0)
else:
    print('Done!!')