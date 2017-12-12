from __future__ import print_function
import json
import imageio as io
from helpers.meanshift import MShift
from helpers.request_parser import RequestParser
from helpers.glad import GLAD
from helpers.aws import AWS
import boto3
import logger


S3_DOWNLOAD_FOLDER='/tmp/'
DYNAMODB='dynamodb'
S3='s3'
#
#   MAIN (SETUP)
#
if __name__ == "__main__":
    import local_env
    import argparse
    parser=argparse.ArgumentParser(description='CLUSTER LOCAL')
    parser.add_argument('data',help='request data as json string')
    parser.add_argument('-e','--env',default='dev',help='local env name. defaults to dev')
    args=parser.parse_args()
    local_env.export(args.env)


#
# PUBLIC METHODS
#
def meanshift(event, context):
    req=RequestParser(event)
    aws=AWS(req.table_name,req.bucket)
    if not req.url: aws.s3.download(req.file)
    # get data
    glad=GLAD(
        data_path=req.data_path,
        start_date=req.start_date
        end_date=req.end_date)
    # run cluster
    mshift=MShift(
        glad.data(),
        width=req.width,
        min_count=req.min_count,
        intensity_threshold=req.intensity_threshold
        iterations=req.iterations)
    # output
    data=req.data()
    data['input_data']=mshift.input_data()
    data['clusters']=mshift.clusters()
    data['nb_clusters']=len(data['clusters'])
    # save data
    aws.db.put(data)
    # response
    body = {
        "event": "{}".format(event),
        "message": "FOUND {} CLUSTERS".format(data['nb_clusters']),
        "table_data": data,
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response



#
#   MAIN (RUN)
#
if __name__ == "__main__":
    logger.out("\nRUN CLUSTER:\t{}".format(args.data))
    logger.out(meanshift(json.loads(args.data),None))


