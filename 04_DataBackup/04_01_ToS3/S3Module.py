import boto3, Config
from pprint import pprint

# s3 적재용 Function
def deleteDataFromBackup(s3,vFilePath):
    response = s3.list_objects_v2(Bucket=Config.BaseConfig.vS3Bucket, Prefix=Config.BaseConfig.vS3Key + "/" + vFilePath)

    if response['KeyCount'] >= 1:
        print("============>> DDB Backup Folder Truncate Start")
        for row in response['Contents']:
            response = s3.delete_object(
                Bucket=Config.BaseConfig.vS3Bucket,
                Key=row['Key'],
            )
            print("{0} has been deleted.".format(row['Key']))
        print("============>> DDB Backup Folder Truncate Done")

def saveDataToS3(s3, vTableNm, vData):
    try:
        response = s3.put_object(Bucket=Config.BaseConfig.vS3Bucket, Key=Config.BaseConfig.vS3Key+"/"+vTableNm, Body=vData)
    except KeyboardInterrupt:
        print("============>> Cancel by user request.")
        exit(0)

def getDataFromS3(s3, vTableNm):
    try:
        response = s3.get_object(Bucket=Config.BaseConfig.vS3Bucket, Key=Config.BaseConfig.vS3Key+"/"+vTableNm)
    except KeyboardInterrupt:
        print("============>> Cancel by user request.")
        exit(0)
    
    return response['Body'].read()