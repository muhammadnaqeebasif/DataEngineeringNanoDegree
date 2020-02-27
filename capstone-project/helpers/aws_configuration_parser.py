import configparser
import  pandas as pd

# Reading the resources file
RESOURCES_CONF='./credentials/resources.cfg'
config = configparser.ConfigParser()
config.read(RESOURCES_CONF)

# AWS Credentials
credentials = pd.read_csv('./credentials/credentials.csv')
ACCESS_KEY = credentials['Access key ID'][0]
SECRET_KEY = credentials['Secret access key'][0]

# Getting the configuration
KINESIS = config['KINESIS']
FIREHOSE = config['FIREHOSE']
REGION = config['AWS']['REGION']
S3=config['S3']
LAMBDA = config['LAMBDA']
REDSHIFT = config['REDSHIFT']


