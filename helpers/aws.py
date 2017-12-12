S3_DOWNLOAD_FOLDER='/tmp/'
DYNAMODB='dynamodb'
S3='s3'


class AWS(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,table_name,bucket):
        self.table_name=table_name
        self.bucket=bucket

        
    def s3_download(self,file):
        self.download_path='{}/{}'.format(S3_DOWNLOAD_FOLDER,file)
        return s3.download_file(self.bucket,file,download_path)
         

    def db_put(self,data):
        self.db_data=data
        table=boto3.resource(DYNAMODB).Table(self.table_name)
        return table.put_item(Item=data)
