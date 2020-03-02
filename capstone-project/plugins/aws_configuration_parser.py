import configparser
import pandas as pd

class AwsConfigs:
    def __init__(self,file_credentials, file_resources):
        # Reading the resources file
        config = configparser.ConfigParser()
        config.read(file_resources)

        # AWS Credentials
        credentials = pd.read_csv(file_credentials)
        self.ACCESS_KEY = credentials['Access key ID'][0]
        self.SECRET_KEY = credentials['Secret access key'][0]

        # Getting the configuration
        self.KINESIS = config['KINESIS']
        self.FIREHOSE = config['FIREHOSE']
        self.REGION = config['AWS']['REGION']
        self.S3=config['S3']
        self.LAMBDA = config['LAMBDA']
        self.REDSHIFT = config['REDSHIFT']
