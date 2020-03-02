import boto3
from plugins.aws_configuration_parser import *
import time

if __name__ == '__main__':

    # creating aws configuration object
    aws_configs = AwsConfigs('dags/credentials/credentials.csv', 'dags/credentials/resources.cfg')
    #------------------------------------Creating Clients--------------------------------------------------------------
    # Creating ec2 resource
    ec2 = boto3.resource('ec2',
                         region_name=aws_configs.REGION,
                         aws_access_key_id=aws_configs.ACCESS_KEY,
                         aws_secret_access_key=aws_configs.SECRET_KEY
                         )

    # Creating s3 resource
    s3 = boto3.resource('s3',
                        region_name=aws_configs.REGION,
                        aws_access_key_id=aws_configs.ACCESS_KEY,
                        aws_secret_access_key=aws_configs.SECRET_KEY
                        )

    # Creating kinesis client
    kinesis = boto3.client('kinesis',
                           region_name=aws_configs.REGION,
                           aws_access_key_id=aws_configs.ACCESS_KEY,
                           aws_secret_access_key=aws_configs.SECRET_KEY
                           )

    # Creating iam
    iam = boto3.client('iam',
                       region_name=aws_configs.REGION,
                       aws_access_key_id=aws_configs.ACCESS_KEY,
                       aws_secret_access_key=aws_configs.SECRET_KEY
                       )

    # Creating cloudwatch client
    cloud_watch = boto3.client('logs',
                               region_name=aws_configs.REGION,
                               aws_access_key_id=aws_configs.ACCESS_KEY,
                               aws_secret_access_key=aws_configs.SECRET_KEY
                               )

    # Creating firehose client
    firehose = boto3.client('firehose',
                            region_name=aws_configs.REGION,
                            aws_access_key_id=aws_configs.ACCESS_KEY,
                            aws_secret_access_key=aws_configs.SECRET_KEY
                            )

    # Creating Lambda client
    lambda_client = boto3.client('lambda',
                                 region_name=aws_configs.REGION,
                                 aws_access_key_id=aws_configs.ACCESS_KEY,
                                 aws_secret_access_key=aws_configs.SECRET_KEY
                                 )
    # Creating redshift client
    redshift = boto3.client('redshift',
                            region_name=aws_configs.REGION,
                            aws_access_key_id=aws_configs.ACCESS_KEY,
                            aws_secret_access_key=aws_configs.SECRET_KEY
                            )
    #-------------------------------------------------------------------------------------------------------------------
    #-------------------------------Deleting Firehose Stream------------------------------------------------------------
    try:
        # Deletes Firehose delivery stream
        firehose.delete_delivery_stream(DeliveryStreamName=aws_configs.FIREHOSE['DELIVERY_STREAM_NAME'],
                                        AllowForceDelete=True
                                        )
        print(f"Firehose Delivery Stream {aws_configs.FIREHOSE['DELIVERY_STREAM_NAME']} is deleted")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    # -------------------------------------------Deleting Lambda function-----------------------------------------------
    try:
        # Removing the lambda function events
        kinesis_stream_arn = kinesis.describe_stream(StreamName=aws_configs.KINESIS['STREAM_NAME'])['StreamDescription']['StreamARN']
        paginator = lambda_client.get_paginator('list_event_source_mappings')
        UUID = list(paginator.paginate(EventSourceArn=kinesis_stream_arn,
                                    FunctionName=aws_configs.LAMBDA['FUNCTION_NAME']))[0]['EventSourceMappings'][0]['UUID']
        lambda_client.delete_event_source_mapping(UUID=UUID)
        # Removing the lambda function
        lambda_client.delete_function(FunctionName=aws_configs.LAMBDA['FUNCTION_NAME'])

        print(f"Lambda Function {aws_configs.LAMBDA['FUNCTION_NAME']} is deleted")
    except Exception as e:
        print(e)
    #------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------Deleting Data Stream--------------------------------------------------
    try:
        # Deletes kinesis data stream
        kinesis.delete_stream(StreamName=aws_configs.KINESIS['STREAM_NAME'])

        print(f"Kinesis Stream {aws_configs.KINESIS['STREAM_NAME']} is deleted")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #-------------------------------------------Creating Cloud Watch Log Group------------------------------------------
    try:
        # Deletes cloud watch stream
        cloud_watch.delete_log_stream(logGroupName=f"/aws/kinesisfirehose/{aws_configs.FIREHOSE['DELIVERY_STREAM_NAME']}",
                                      logStreamName='S3Stream')
        print(f"Cloud watch group is deleted")
    except Exception as e:
        print(e)

    try:
        # Deletes cloud watch group
        cloud_watch.delete_log_group(logGroupName=f"/aws/kinesisfirehose/{aws_configs.FIREHOSE['DELIVERY_STREAM_NAME']}")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #---------------------------------------------Deleting S3 Bucket---------------------------------------------------
    # try:
    #     # Creates a reference to S3 Bucket
    #     bucket = s3.Bucket(aws_configs.S3['BUCKET'])
    #     # Removes all the keys inside the bucket
    #     for key in bucket.objects.all():
    #         key.delete()
    #     # Deletes the bucket
    #     bucket.delete()
    #     print(f"S3 Bucket {aws_configs.S3['BUCKET']} is deleted")
    # except Exception as e:
    #     print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------Deletomg the redshift cluster------------------------------------------
    try:
        redshift.delete_cluster(ClusterIdentifier=aws_configs.REDSHIFT['CLUSTER_IDENTIFIER'], SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)

    # Wait untill the cluster is deleted
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=aws_configs.REDSHIFT['CLUSTER_IDENTIFIER'])['Clusters'][0]
        while myClusterProps['ClusterAvailabilityStatus'] != 'Deleting':
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=aws_configs.REDSHIFT['CLUSTER_IDENTIFIER'])['Clusters'][0]
            time.sleep(10)
    except:
        print("Redshift cluster deleted")

    #-------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------Deleting the IAM roles-------------------------------------------------
    try:
        # Detaching the policies from FIREHOSE role
        iam.delete_role_policy(RoleName=aws_configs.FIREHOSE['ROLE_NAME'],
                               PolicyName=aws_configs.FIREHOSE['POLICY']
                               )
        # Detaching the policies from Lambda role
        iam.delete_role_policy(RoleName=aws_configs.LAMBDA['ROLE_NAME'],
                               PolicyName=aws_configs.LAMBDA['POLICY'])

        # Detaching the policies from redshift role
        iam.detach_role_policy(RoleName=aws_configs.REDSHIFT['ROLE_NAME'], PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print(e)

    try:
        # Deletes the FIREHOSE role
        iam.delete_role(RoleName=aws_configs.FIREHOSE['ROLE_NAME'])

        # Deletes the lambda role
        iam.delete_role(RoleName=aws_configs.LAMBDA['ROLE_NAME'])

        # Deleting the redshift role
        iam.delete_role(RoleName=aws_configs.REDSHIFT['ROLE_NAME'])

        print("Removed all roles")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
