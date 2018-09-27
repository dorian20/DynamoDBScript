# mjlee

from pprint import pprint
import boto3, sys, botocore, re

vSrcProfileName = 'l-ellotte-dev'
vTgtProfileName = 'l-ellotte-tst'
vSrcRegionName = 'ap-northeast-2'
vTgtRegionName = 'ap-northeast-2'

session = boto3.Session(profile_name=vSrcProfileName, region_name=vSrcRegionName)
dynamodb = session.client('dynamodb', region_name=vSrcRegionName)

session2 = boto3.Session(profile_name=vTgtProfileName, region_name=vTgtRegionName)
dynamodb2 = session2.client('dynamodb', region_name=vTgtRegionName)


# 테이블 이름에 postfix를 붙이고 싶을 때 사용한다.
tblNmPostFix = ""

try:
    word = sys.argv[1]
    print ("Table Replication Start : " + word + "%")
except IndexError:
    word = "ALL"
    print ("Table Replication Start : ALL MODE")

vYesNo = input("Do you want to start replication(Y/[N])?")
if vYesNo is None or vYesNo == "N" or vYesNo == "n" or vYesNo == "":
    print("Good Bye")
    exit(0)

#함부로 수행하면 위험하므로 exit(0)로 처리
#실제 수행할 경우는 아래의 exit(0)을 제거 후 사용
exit(0)

def fn_CreTbl(TableName, AttributeDefinitions, KeySchema, ProvisionedThroughput, LocalSecondaryIndexes, GlobalSecondaryIndexes):
    try:
        #GSI, LSI 둘다 존재하지 않을 때
        if LocalSecondaryIndexes is None and GlobalSecondaryIndexes is None:
            vResult = dynamodb2.create_table(TableName=TableName,
                AttributeDefinitions=AttributeDefinitions,
                KeySchema=KeySchema,
                ProvisionedThroughput=ProvisionedThroughput
                )
        #GSI만 존재할 때
        elif LocalSecondaryIndexes is None:
            vResult = dynamodb2.create_table(TableName=TableName,
                AttributeDefinitions=AttributeDefinitions,
                KeySchema=KeySchema,
                ProvisionedThroughput=ProvisionedThroughput,
                GlobalSecondaryIndexes=GlobalSecondaryIndexes
                )
        #LSI만 존재할 때
        elif GlobalSecondaryIndexes is None:
            vResult = dynamodb2.create_table(TableName=TableName,
                AttributeDefinitions=AttributeDefinitions,
                KeySchema=KeySchema,
                ProvisionedThroughput=ProvisionedThroughput,
                LocalSecondaryIndexes=LocalSecondaryIndexes
                )
        else:
            vResult = dynamodb2.create_table(TableName=TableName,
                AttributeDefinitions=AttributeDefinitions,
                KeySchema=KeySchema,
                ProvisionedThroughput=ProvisionedThroughput,
                LocalSecondaryIndexes=LocalSecondaryIndexes,
                GlobalSecondaryIndexes=GlobalSecondaryIndexes
                )
        return vResult
    except dynamodb.exceptions.ResourceInUseException:
        pprint(NewTName + ' exists...')
        pass
        return



for tName in dynamodb.list_tables()['TableNames']:
    # 전체를 수행하고 싶을 때는 word에 ALL을 넣고 수행한다.
    # word in tName 수정할 것 if re.match('^'+word+'', tName):

    if (re.match('^'+word+'', tName) and word is not None and word != '') or word == "ALL":
        print(tName)
        #if target dynamodb에 tName이 존재할 경우 Pass 시킨다.


        TabMeta = dynamodb.describe_table(TableName=tName)['Table']
        TTLMeta = dynamodb.describe_time_to_live(TableName=tName)

        #새로운 테이블명
        NewTName = tName + tblNmPostFix

        #테이블명이 존재하면 try catch로 잡아서 continue
        try:
            IsTabCreated = dynamodb2.describe_table(TableName=NewTName)
            pprint(NewTName + " is exists...")
            continue
        except Exception as ex:
            pass

        # Table Level ProvisionedThroughput 에서 불필요한 칼럼을 제거한다.
        TabMeta['ProvisionedThroughput'].pop('NumberOfDecreasesToday',None)

        NewTabMeta = {}
        NewTabMeta['TableName'] = TabMeta['TableName']
        NewTabMeta['AttributeDefinitions'] = TabMeta['AttributeDefinitions']
        NewTabMeta['KeySchema'] = TabMeta['KeySchema']
        NewTabMeta['ProvisionedThroughput'] = TabMeta['ProvisionedThroughput']
        NewTabMeta['ProvisionedThroughput'].pop('LastDecreaseDateTime',None)
        NewTabMeta['ProvisionedThroughput'].pop('LastIncreaseDateTime',None)

        # StreamSpecification 값이 존재하면 입력한다.
        #NewTabMeta['StreamSpecification'] = {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}

        isStream = False

        try:
            isStream = True
            NewTabMeta['StreamSpecification'] = TabMeta['StreamSpecification']
        except KeyError:
            isStream = False
            pass

        # GSI를 추가해주는 부분
        try:
            NewTabMeta['GlobalSecondaryIndexes'] = []
            for vGsi in TabMeta['GlobalSecondaryIndexes']:
                # GSI Meta에서 필요 없는 부분 제거한다.
                vGsi.pop("IndexStatus",None)
                vGsi.pop("IndexSizeBytes",None)
                vGsi.pop("ItemCount",None)
                vGsi.pop("IndexArn",None)
                # GSI Level ProvisionedThroughput 에서 불필요한 칼럼을 제거한다.
                vGsi['ProvisionedThroughput'].pop("NumberOfDecreasesToday",None)
                vGsi['ProvisionedThroughput'].pop('LastDecreaseDateTime',None)
                vGsi['ProvisionedThroughput'].pop('LastIncreaseDateTime',None)

                NewTabMeta['GlobalSecondaryIndexes'].append(vGsi)
        except KeyError:
            NewTabMeta['GlobalSecondaryIndexes'] = None
            pass

        # LSI를 추가해주는 부분
        try:
            NewTabMeta['LocalSecondaryIndexes'] = []
            for vGsi in TabMeta['LocalSecondaryIndexes']:
                # LSI Meta에서 필요 없는 부분 제거한다.
                vGsi.pop("IndexStatus",None)
                vGsi.pop("IndexSizeBytes",None)
                vGsi.pop("ItemCount",None)
                vGsi.pop("IndexArn",None)
                NewTabMeta['LocalSecondaryIndexes'].append(vGsi)
        except KeyError:
            NewTabMeta['LocalSecondaryIndexes'] = None
            pass

        try:
            result = fn_CreTbl(TableName=NewTName,
                AttributeDefinitions=TabMeta['AttributeDefinitions'],
                KeySchema=TabMeta['KeySchema'],
                ProvisionedThroughput=TabMeta['ProvisionedThroughput'],
                LocalSecondaryIndexes=NewTabMeta['LocalSecondaryIndexes'],
                GlobalSecondaryIndexes=NewTabMeta['GlobalSecondaryIndexes']
                )

            if result['ResponseMetadata']['HTTPStatusCode'] == 200:

                #TTL 추가
                if TTLMeta['TimeToLiveDescription']['TimeToLiveStatus'] == 'ENABLED' or isStream:
                    # 테이블이 생성되기를 기다린다. 5초씩 100번 시도
                    print('Waiting for creation of table')
                    waiter = dynamodb2.get_waiter('table_exists')
                    waiter.wait(
                        TableName=NewTName,
                        WaiterConfig={
                            'Delay': 5,
                            'MaxAttempts': 100
                        }
                    )

                    if TTLMeta['TimeToLiveDescription']['TimeToLiveStatus'] == 'ENABLED':
                        NewTTLMeta = {}
                        NewTTLMeta['Enabled'] = True
                        NewTTLMeta['AttributeName'] = TTLMeta['TimeToLiveDescription']['AttributeName']
                        # TTL 을 갱신한다.
                        resultTTL = dynamodb2.update_time_to_live(TableName=NewTName,TimeToLiveSpecification=NewTTLMeta)
                        print('TTL is enabled')

                    if isStream:
                        dynamodb2.update_table(TableName=NewTName, StreamSpecification=NewTabMeta['StreamSpecification'])
                        print('Dynamo Stream is enabled')

            else:
                print(NewTName + ' creation failed...')
        except dynamodb.exceptions.ResourceInUseException:
            pprint(NewTName + ' exists...')
            pass

print('Done!')

