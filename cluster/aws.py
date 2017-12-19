import boto3



class AWS(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name,bucket):
        self.s3=S3(bucket)
        self.db=DynamoDB(table_name)




class S3(object):
    CLIENT_NAME='s3'
    #
    # PUBLIC METHODS
    #
    def __init__(self,bucket):
        self.bucket=bucket


    def download(self,file,download_path):
        client=boto3.resource(self.CLIENT_NAME).meta.client
        return client.download_file(self.bucket,file,download_path)




class DynamoDB(object):
    CLIENT_NAME='dynamodb'
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name):
        self.table_name=table_name


    def put(self,data):
        self.db_data=data
        table=boto3.resource(self.CLIENT_NAME).Table(self.table_name)
        return table.put_item(Item=data)