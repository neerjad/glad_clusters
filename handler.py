from __future__ import print_function
import json
import logging
import imageio as io
from clusters.meanshift import MShift
from clusters.request_parser import RequestParser
import clusters.processors as proc
from clusters.aws import AWS
#
# LOGGING CONFIG
#
FORMAT='%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#
# PUBLIC METHODS
#
def meanshift(event, context):
    req=RequestParser(event)
    aws=AWS(req.table_name,req.bucket)
    if not req.url: aws.s3.download(req.file_name,req.data_path)
    # load/process data
    im_data=io.imread(req.data_path)
    if req.preprocess_data:
        im_data=proc.glad_between_dates(
            im_data,
            req.start_date,
            req.end_date)
    if req.intensity_threshold:
        im_data=proc.threshold(
            im_data,
            threshold=req.intensity_threshold,
            hard_threshold=req.hard_threshold)
    # run cluster
    mshift=MShift(
        data=im_data,
        width=req.width,
        min_count=req.min_count,
        iterations=req.iterations,
        downsample=req.downsample)
    # output
    data=req.data()
    data['input_data']=MShift.zero_shifted_list(mshift.centered_data())
    data['clusters']=MShift.zero_shifted_list(mshift.clusters())
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
    import local_env
    import argparse
    parser=argparse.ArgumentParser(description='CLUSTER LOCAL')
    parser.add_argument('data',help='request data as json string')
    parser.add_argument('-e','--env',default='dev',help='local env name. defaults to dev')
    args=parser.parse_args()
    local_env.export(args.env)
    logger.info("\nRUN CLUSTER:\t{}".format(args.data))
    logger.info(meanshift(json.loads(args.data),None))


