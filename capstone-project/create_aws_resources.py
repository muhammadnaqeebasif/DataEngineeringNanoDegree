from aws_configuration_parser import *
import boto3
import json
import time
import zipfile

if __name__ == '__main__':
    #---------------------------- Creating clients----------------------------------------------------------------------
    # Creating ec2 resource
    ec2 = boto3.resource('ec2',
                         region_name=REGION,
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET_KEY
                         )

    # Creating s3 resource
    s3 = boto3.resource('s3',
                        region_name=REGION,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY
                        )

    # Creating kinesis client
    kinesis = boto3.client('kinesis',
                           region_name=REGION,
                           aws_access_key_id=ACCESS_KEY,
                           aws_secret_access_key=SECRET_KEY
                           )

    # Creating iam
    iam = boto3.client('iam',
                       region_name=REGION,
                       aws_access_key_id=ACCESS_KEY,
                       aws_secret_access_key=SECRET_KEY
                       )

    # Creating cloudwatch client
    cloud_watch = boto3.client('logs',
                               region_name=REGION,
                               aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY
                               )

    # Creating firehose client
    firehose = boto3.client('firehose',
                            region_name=REGION,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY
                            )

    # Creating Lambda client
    lambda_client = boto3.client('lambda',
                                 region_name=REGION,
                                 aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_KEY
                                 )
    # Creating redshift client
    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY
                            )

    #-------------------------------------------------------------------------------------------------------------------
    #----------------------------------- Creating Roles-------------------------------------------------------------
    try:
        # creating firehose delivery role
        iam.create_role(Path='/',
                        RoleName=FIREHOSE['ROLE_NAME'],
                        Description='Allows Redshift clusters to call AWS services on your behalf.',
                        AssumeRolePolicyDocument=json.dumps({
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "firehose.amazonaws.com"
                                    },
                                    "Action": "sts:AssumeRole",
                                }
                            ]
                        }))
        print(f"Firehose IAM Role {FIREHOSE['ROLE_NAME']} is created")
    except Exception as e:
        print(e)

    # Get Account ID
    ACCOUNT_ID = boto3.client('sts',
                              region_name=REGION,
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY
                              ).get_caller_identity().get('Account')

    # creating firehose policy
    firehose_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions"
                ],
                "Resource": "*"
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3['BUCKET']}",
                    f"arn:aws:s3:::{S3['BUCKET']}/*",
                    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%",
                    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%/*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "lambda:InvokeFunction",
                    "lambda:GetFunctionConfiguration"
                ],
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:%FIREHOSE_DEFAULT_FUNCTION%:%FIREHOSE_DEFAULT_VERSION%"
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/kinesisfirehose/{FIREHOSE['DELIVERY_STREAM_NAME']}:log-stream:*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords"
                ],
                "Resource": f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{KINESIS['STREAM_NAME']}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "kms:Decrypt"
                ],
                "Resource": [
                    f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/%SSE_KEY_ID%"
                ],
                "Condition": {
                    "StringEquals": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com"
                    },
                    "StringLike": {
                        "kms:EncryptionContext:aws:kinesis:arn": f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{KINESIS['STREAM_NAME']}"
                    }
                }
            }
        ]
    }

    # Attaching policy to firehose role
    iam.put_role_policy(RoleName=FIREHOSE['ROLE_NAME'],
                        PolicyName=FIREHOSE['POLICY'],
                        PolicyDocument=json.dumps(firehose_policy))

    try:
        # Creating Lambda Role
        iam.create_role(Path='/',
                        RoleName=LAMBDA['ROLE_NAME'],
                        AssumeRolePolicyDocument=json.dumps({
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                "Sid": "",
                                "Effect": "Allow",
                                "Principal": {
                                "Service": "lambda.amazonaws.com"
                                },
                                "Action": "sts:AssumeRole"
                                }
                            ]
                            }
                        )
        )
        print(f"Lambda IAM Role {LAMBDA['ROLE_NAME']} is created")
    except Exception as e:
        print(e)

    # Attaching the policy to lambda role
    iam.put_role_policy(RoleName=LAMBDA['ROLE_NAME'],
                        PolicyName=LAMBDA['POLICY'],
                        PolicyDocument=json.dumps({
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "kinesis:DescribeStream",
                                        "kinesis:DescribeStreamSummary",
                                        "kinesis:GetRecords",
                                        "kinesis:GetShardIterator",
                                        "kinesis:ListShards",
                                        "kinesis:ListStreams",
                                        "kinesis:SubscribeToShard",
                                        "logs:CreateLogGroup",
                                        "logs:CreateLogStream",
                                        "logs:PutLogEvents",
                                        "lambda:InvokeFunction",
                                        "s3:*"
                                    ],
                                    "Resource": "*"
                                }
                            ]
                        })
    )

    try:
        # Creating Redshift role
        iam.create_role(
            Path='/',
            RoleName=REDSHIFT['ROLE_NAME'],
            Description='Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        print(f"Redshift IAM Role {REDSHIFT['ROLE_NAME']} is created")
    except Exception as e:
        print(e)

    # Attaching policy to redshift role
    iam.attach_role_policy(RoleName=REDSHIFT['ROLE_NAME'],
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )

    #------------------------------------------------------------------------------------------------------------------
    #------------------------------------------------Creating S3 Bucket------------------------------------------------
    try:
        # Creates s3 bucket
        s3.create_bucket(Bucket=S3['BUCKET'],
                         CreateBucketConfiguration={
                             'LocationConstraint': REGION}
                         )
        print(f"S3 Bucket {S3['BUCKET']} is created")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #------------------------------------------------Creating Kinesis data stream---------------------------------------
    try:
        # Creating kinesis data streams
        kinesis.create_stream(StreamName=KINESIS['STREAM_NAME'],
                                     ShardCount=int(KINESIS['SHARD_COUNT']))
        print(f"Kinesis Stream {KINESIS['STREAM_NAME']} is created")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #------------------------------------------Creating Cloud Watch logs------------------------------------------------
    try:
        # Creating cloudwatch group
        cloud_watch.create_log_group(logGroupName=f"/aws/kinesisfirehose/{FIREHOSE['DELIVERY_STREAM_NAME']}")

        # Creating cloudwatch stream
        cloud_watch.create_log_stream(
            logGroupName=f"/aws/kinesisfirehose/{FIREHOSE['DELIVERY_STREAM_NAME']}",
            logStreamName='S3Stream'
        )
        print(f"Cloudwatch group /aws/kinesisfirehose/{FIREHOSE['DELIVERY_STREAM_NAME']} is created")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #----------------------------------------Creating Firehose Delivery Stream------------------------------------------
    # Putting a delay of 30 secs in order to propagate the role successfully
    time.sleep(30)

    # Getting the ARN for firehose role
    firehose_role_arn = iam.get_role(RoleName=FIREHOSE['ROLE_NAME'])['Role']['Arn']
    #
    # # Getting the ARN of kinesis stream
    kinesis_stream_arn = kinesis.describe_stream(StreamName=KINESIS['STREAM_NAME'])['StreamDescription']['StreamARN']
    try:
        firehose.create_delivery_stream(
            DeliveryStreamName=FIREHOSE['DELIVERY_STREAM_NAME'],
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': kinesis_stream_arn,
                'RoleARN': firehose_role_arn
            },
            S3DestinationConfiguration={
                'RoleARN': firehose_role_arn,
                'BucketARN': f"arn:aws:s3:::{S3['BUCKET']}",
                'Prefix': 'streamed_data-',
                'BufferingHints': {
                    'SizeInMBs': 5,
                    'IntervalInSeconds': 60
                },
                'CompressionFormat': 'UNCOMPRESSED',
                'EncryptionConfiguration': {
                    'NoEncryptionConfig': 'NoEncryption'
                },
                'CloudWatchLoggingOptions': {
                    'Enabled': True,
                    'LogGroupName': f"/aws/kinesisfirehose/{FIREHOSE['DELIVERY_STREAM_NAME']}",
                    'LogStreamName': 'S3Stream'

                }
            },

        )
        print(f"Firehose Delivery Stream {FIREHOSE['DELIVERY_STREAM_NAME']} is created")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #-------------------------------------------Creating Lambda Function------------------------------------------------
    # Creating zip file for lambda function
    zf = zipfile.ZipFile('lambda.zip',mode='w')
    try:
        zf.write('lambda_function.py')
    finally:
        zf.close()

    # Getting the role
    lambda_role_arn = iam.get_role(RoleName=LAMBDA['ROLE_NAME'])['Role']['Arn']

    with open('lambda.zip', 'rb') as f:
        zipped_code = f.read()
    try:
        # Creating Lambda function
        lambda_client.create_function(
            FunctionName=LAMBDA['FUNCTION_NAME'],
            Runtime='python3.7',
            Role=lambda_role_arn,
            Handler='lambda_function.lambda_handler',
            Code=dict(ZipFile=zipped_code),
            Timeout=300,  # Maximum allowable timeout
            Environment={
                'Variables': {
                    'S3Bucket': S3['Bucket'],
                    'output_key_prefix' : S3['real_processed_key']
                    }
                }
        )

        # Creating Kinesis Trigger
        lambda_client.create_event_source_mapping(
            EventSourceArn=kinesis_stream_arn,
            FunctionName=LAMBDA['FUNCTION_NAME'],
            Enabled=True,
            BatchSize=100,
            StartingPosition='LATEST',
            MaximumRetryAttempts=123
        )
        print(f"Lambda function {LAMBDA['FUNCTION_NAME']} is created")
    except Exception as e:
        print(e)
    #-------------------------------------------------------------------------------------------------------------------
    #-----------------------------------------Creating RedShift Cluster-------------------------------------------------
    redshift_role_arn = iam.get_role(RoleName=REDSHIFT['ROLE_NAME'])['Role']['Arn']

    try:

        # creating rdshift cluster
        response = redshift.create_cluster(
            # HW
            ClusterType=REDSHIFT['CLUSTER_TYPE'],
            NodeType=REDSHIFT['NODE_TYPE'],
            NumberOfNodes=int(REDSHIFT['NUM_NODES']),

            # Identifiers & Credentials
            DBName=REDSHIFT['DB_NAME'],
            ClusterIdentifier=REDSHIFT['CLUSTER_IDENTIFIER'],
            MasterUsername=REDSHIFT['DB_USER'],
            MasterUserPassword=REDSHIFT['DB_PASSWORD'],

            # Roles (for s3 access)
            IamRoles=[redshift_role_arn]
        )
    except Exception as e:
        print(e)

    # Checking if redshift cluster becomes available or not
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=REDSHIFT['CLUSTER_IDENTIFIER'])['Clusters'][0]
    while myClusterProps['ClusterAvailabilityStatus'] != 'Available':
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=REDSHIFT['CLUSTER_IDENTIFIER'])['Clusters'][0]
        time.sleep(10)
    print(f"Redshift cluster {REDSHIFT['CLUSTER_IDENTIFIER']} is created")
    # Open an incoming  TCP port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(REDSHIFT['PORT']),
            ToPort=int(REDSHIFT['PORT'])
        )
    except Exception as e:
        print(e)

    # Adding end point configuration of the cluster to the configuration file
    config = configparser.ConfigParser()
    config.read('credentials/resources.cfg')

    config['REDSHIFT']['ENDPOINT'] = myClusterProps['Endpoint']['Address']
    config['REDSHIFT']['ROLE_ARN'] = redshift_role_arn

    # writing to the configuration file
    with open('credentials/resources.cfg', 'w') as config_file:
        config.write(config_file)
