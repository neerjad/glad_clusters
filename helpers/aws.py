import boto3



class AWS(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name,bucket):
        self.s3=S3(bucket)
        self.db=DynamoDB(table_name)

s3_client=boto3.resource('s3')


class S3(object):
    DOWNLOAD_FOLDER='/tmp'
    CLIENT_NAME='s3'
    #
    # PUBLIC METHODS
    #
    def __init__(self,bucket,ext='png'):
        self.bucket=bucket
        self.ext=ext


    def download(self,file):
        client=boto3.resource(self.CLIENT_NAME)
        self.download_path='{}/{}'.format(self.DOWNLOAD_FOLDER,file)
        if self.ext:
           self.download_path='{}.{}'.format(self.download_path,self.ext) 
        print(self.bucket,file,self.download_path)
        return s3_client.download_file(self.bucket,file,self.download_path)




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