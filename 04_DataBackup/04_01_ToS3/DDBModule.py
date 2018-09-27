import datetime, S3Module, json, time
from pprint import pprint
from botocore.exceptions import ClientError

now = datetime.datetime.now()

def backup(vTableNm, vLimit, vSleepTime, ddb, s3):
    print('################ BEGIN => {0} ##################'.format(vTableNm))
    vFileNo = 1
    vFilePath = now.strftime('%Y/%m/%d') + "/" + vTableNm
    vFileNm = '{0}_{1:03d}.json'.format(vTableNm, vFileNo)
    vFilePathAndNm = vFilePath + "/" + vFileNm

    vFirstRunFlag = True
    # 여기서 수행 실패할 수 있음..
    ddb.describe_limits()
    exit(0)
    while vFirstRunFlag:
        try:
            vDatas = ddb.scan(TableName=vTableNm,Limit=vLimit, ReturnConsumedCapacity='INDEXES')#, ExclusiveStartKey={"GOODS_NO" : {"S":"1000003484"}})
            vFirstRunFlag = False
        except KeyboardInterrupt:
            print("============>> Cancel by user request.")
            exit(0)
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                print("============>> ProvisionedThroughputExceeded!!!! Wait 10s")
                time.sleep(10)
                print("============>> ProvisionedThroughputExceeded!!!! Retry")
                continue
            else:
                print("============>> ERROR!!!!!!!!!!!!!!!!!!! ")
                print(str(ce.response['Error']['Code']))
            exit(0)        

    vItems = vDatas['Items']
    vLastEvalKey = vDatas.pop('LastEvaluatedKey',None)

    # 해당 폴더에 파일 존재하면 지울 필요 있음.. 파일 전체 삭제...
    S3Module.deleteDataFromBackup(s3, vFilePath)

    # 최초 수행이며, LastEvaluatedKey가 있으면 별도 While 수행 필요함
    # S3Module.saveDataToS3(s3, vFilePathAndNm, json.dumps(vItems))
    S3Module.saveDataToS3(s3, vFilePathAndNm, str(vItems))
    vProcessedCount = vDatas['ScannedCount']
    vConsumedCapacity = vDatas['ConsumedCapacity']['Table']['CapacityUnits']
    #print('{0} : {1} -> {2}'.format(vTableNm, vProcessedCount, vFilePathAndNm))
    #pprint(vDatas)
    print('{0} : {1} -> {2} / Sleeping {3} / RCU {4}'.format(vTableNm, vProcessedCount, vFilePathAndNm, vSleepTime, vConsumedCapacity))
    time.sleep(vSleepTime)
    while vLastEvalKey is not None:
        vFileNo = vFileNo + 1
        vFilePathAndNm = '{0}{1}/{1}_{2:03d}.json'.format(now.strftime('%Y/%m/%d/'), vTableNm, vFileNo)
        try:
            vDatas = ddb.scan(TableName=vTableNm, Limit=vLimit, ExclusiveStartKey=vLastEvalKey, ReturnConsumedCapacity='INDEXES')
        except KeyboardInterrupt:
            print("============>> Cancel by user request.")
            exit(0)
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                print("============>> ProvisionedThroughputExceeded!!!! Wait : 10s")
                time.sleep(10)
                continue
            else:
                print("============>> ERROR!!!!!!!!!!!!!!!!!!! ")
                print(str(ce.response['Error']['Code']))
            exit(0)
        
        vItems = vDatas['Items']
        vLastEvalKey = vDatas.pop('LastEvaluatedKey',None)
        S3Module.saveDataToS3(s3, vFilePathAndNm, json.dumps(vItems))
        vProcessedCount = vProcessedCount + vDatas['ScannedCount']
        vConsumedCapacity = vDatas['ConsumedCapacity']['Table']['CapacityUnits']
        print('{0} : {1} -> {2} / Sleeping {3} / RCU {4}'.format(vTableNm, vProcessedCount, vFilePathAndNm, vSleepTime, vConsumedCapacity))
        time.sleep(vSleepTime)
    print('################ END => {0} ##################\n'.format(vTableNm))

    return