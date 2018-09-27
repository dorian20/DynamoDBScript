# -*- coding: utf-8 -*-
# DataBackup To S3
import argparse, boto3, json, S3Module, DDBModule, re, Config
from botocore.exceptions import ClientError
from pprint import pprint

parser = argparse.ArgumentParser(description="DataBackup For DynamoDB To S3")

parser.add_argument('-m','--mode', action='store', dest='mode', help="Backup Type 'ALL' table or specific 'TableName'", default='ALL', type=str)
parser.add_argument('-a','--accesskey', action='store', dest='accesskey', help="AWS Access Key", required=True)
parser.add_argument('-s','--secretkey', action='store', dest='secretkey', help="AWS Secret Key", required=True)
parser.add_argument('-r','--region', action='store', dest='region', help="AWS region(default ap-northeast-2)", default='ap-northeast-2')
parser.add_argument('-l','--limit', action='store', dest='limit', help="For s3 put object's limit size(default 50)", default=50, type=int)
parser.add_argument('-f','--force', action='store_true', help="Force start without user input")
parser.add_argument('-t','--time', action='store', help="Sleep time for prevent ProvisionedThroughputExceededException", default="1", type=float)
parser.add_argument('-b','--bucket', action='store', dest="bucket", help="Target bucket name", default="test.lmj", type=str)
parser.add_argument('-d','--directory', action='store', dest="directory", help="Target directory of bucket", default="lmj/dynamobackup", type=str)


args = parser.parse_args()
vSleepTime = args.time
Config.BaseConfig.vS3Bucket = args.bucket
Config.BaseConfig.vS3Key = args.directory


# force mode is true
if args.accesskey is None or args.secretkey is None:
    exit(0)
else:
    accesskey = args.accesskey
    secretkey = args.secretkey

if args.force:
    print("force starting DynamoDB Backup")
# force mode is false
else:
    vYesNo = input("Do you want to backup {0} data (Y/[N])?".format(args.mode))

    if vYesNo is None or (vYesNo != "Y" and vYesNo != "y") or vYesNo == "":
        print("Good Bye")
        exit(0)

ddb = boto3.client(service_name='dynamodb',
                   aws_access_key_id = accesskey,
                   aws_secret_access_key = secretkey,
                   region_name = args.region)

s3 = boto3.client(service_name='s3',
                  aws_access_key_id = accesskey,
                  aws_secret_access_key = secretkey,
                  region_name = args.region)

vLimit = args.limit

if args.mode == 'ALL':
    vTableList = ddb.list_tables(Limit=100)['TableNames']
else:
    try:
        vTableList = []
        vTableList.append(ddb.describe_table(TableName=args.mode)['Table']['TableName'])
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ResourceNotFoundException':
            print("No Table. Check Again!")
        else:
            print(str(ce.response['Error']['Code']))
        exit(0)

for vTableNm in vTableList:
    if re.match('^ET_|^CH_|^GD_|^DP_|^SC_|^MB_|^CC_|^PR_|^OM_|^SM_|^LO_|^PY_|^CH_|^CM_|^AT_|^PR_|^ST_', vTableNm):
        DDBModule.backup(vTableNm, vLimit, vSleepTime, ddb, s3)
    else:
        print("[" + vTableNm + "] isn't target of ddb backup.")
exit(0)