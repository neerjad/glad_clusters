S3_DOWNLOAD_FOLDER='/tmp/'
DYNAMODB='dynamodb'
S3='s3'


class AWS(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name,bucket):
        self.s3=S3(bucket)
        self.db=DynamoDB(table_name)




class S3(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,bucket):
        self.bucket=bucket


    def download(self,file):
        client=boto3.resource(S3)
        self.download_path='{}/{}'.format(S3_DOWNLOAD_FOLDER,file)
        return self.client.download_file(self.bucket,file,self.download_path)




class DynamoDB(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name):
        self.table_name=table_name


    def put(self,data):
        self.db_data=data
        table=boto3.resource(DYNAMODB).Table(self.table_name)
        return table.put_item(Item=data)