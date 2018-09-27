# DataCopy
# mjlee

import boto3
from pprint import pprint

vProfileName = 'l-ellotte-prd'
vRegionName = 'ap-northeast-2'

session = boto3.Session(profile_name=vProfileName, region_name=vRegionName)
dynamodb = session.client('dynamodb')
autoscaling = session.client('application-autoscaling')

targetTableList = dynamodb.list_tables()['TableNames']
#targetTableList = ["TEST_LMJ_WCU","TEST_KSI","TEST_KSI2"]

targetList = []
for tableName in targetTableList:
    table = dynamodb.describe_table(TableName=tableName)
    targetList.append(('TABLE',tableName))
    indices = table['Table'].get('GlobalSecondaryIndexes')
    if indices is not None:
        for index in indices:
            targetList.append(('INDEX',tableName, index['IndexName']))


for target in targetList:
    if target[0] == "TABLE":
        resourceId = "table/{0}".format(target[1])
        response = autoscaling.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:table:WriteCapacityUnits',
            MinCapacity=5,
            MaxCapacity=1000
        )

        response = autoscaling.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:table:ReadCapacityUnits',
            MinCapacity=5,
            MaxCapacity=1000
        )

        response = autoscaling.put_scaling_policy(
            PolicyName="DynamoDBWriteCapacityUtilization:table/{0}".format(target[1]),
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:table:WriteCapacityUnits',
            PolicyType='TargetTrackingScaling',
            TargetTrackingScalingPolicyConfiguration={
                'TargetValue': 60.0,
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization'
                }
            }
        )
        response = autoscaling.put_scaling_policy(
            PolicyName="DynamoDBReadCapacityUtilization:table/{0}".format(target[1]),
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:table:ReadCapacityUnits',
            PolicyType='TargetTrackingScaling',
            TargetTrackingScalingPolicyConfiguration={
                'TargetValue': 60.0,
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'DynamoDBReadCapacityUtilization'
                }
            }
        )

    else:
        resourceId = "table/{0}/index/{1}".format(target[1],target[2])

        response = autoscaling.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:index:WriteCapacityUnits',
            MinCapacity=5,
            MaxCapacity=1000
        )
        response = autoscaling.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:index:ReadCapacityUnits',
            MinCapacity=5,
            MaxCapacity=1000
        )

        response = autoscaling.put_scaling_policy(
            PolicyName="DynamoDBWriteCapacityUtilization:table/{0}/index/{1}".format(target[1],target[2]),
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:index:WriteCapacityUnits',
            PolicyType='TargetTrackingScaling',
            TargetTrackingScalingPolicyConfiguration={
                'TargetValue': 60.0,
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization'
                }
            }
        )
        response = autoscaling.put_scaling_policy(
            PolicyName="DynamoDBReadCapacityUtilization:table/{0}/index/{1}".format(target[1],target[2]),
            ServiceNamespace='dynamodb',
            ResourceId=resourceId,
            ScalableDimension='dynamodb:index:ReadCapacityUnits',
            PolicyType='TargetTrackingScaling',
            TargetTrackingScalingPolicyConfiguration={
                'TargetValue': 60.0,
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'DynamoDBReadCapacityUtilization'
                }
            }
        )

