from __future__ import print_function
import json
import logging
import imageio as io
from clusters.meanshift import MShift
from clusters.request_parser import RequestParser
import clusters.processors as proc

#
# CONFIG
#
RETURN_EMPTY=False


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
    if req.is_not_valid():
        return _error(req,'request not valid',1)
    else:
        try:
            im_data=_im_data(req)
            if im_data is False:
                return _error(req,'{} not found'.format(req.data_path),2)
            else:
                im_data=_preprocess(req,im_data)
                mshift=MShift(
                    data=im_data,
                    width=req.width,
                    min_count=req.min_count,
                    iterations=req.iterations)
                output_data, nb_clusters=_output_data(req,mshift)
                if (nb_clusters>0) or RETURN_EMPTY:
                    return output_data
                else:
                    return None
        except Exception as e:
            return _error(req,'Exception: {}'.format(e),3)



def _im_data(req):
    if not req.url: _download(req.bucket,req.file_name,req.data_path)
    try:
        return io.imread(req.data_path)
    except Exception as e:
        logger.warn(
            "\nfailed to read image ({}) -- {}".format(req.data_path,e))
        return False


def _preprocess(req,im_data): 
    if req.preprocess_data:
        im_data=proc.glad_between_dates(
            im_data,
            req.start_date,
            req.end_date)
    return im_data


def _output_data(req,mshift):
    data=req.data()
    data['data']=mshift.clusters_data() or {}
    nb_clusters=data['data'].pop('nb_clusters',0)
    data['nb_clusters']=nb_clusters
    return data, nb_clusters


def _error(req,msg,trace_id):
    error={ 'error': msg, 'error_trace':'handler.{}'.format(trace_id) }
    error.update(req.data())
    return error


def _process_response(event,output_data):
    body = {
        "event": "{}".format(event),
        "message": "FOUND {} CLUSTERS".format(output_data['nb_clusters']),
        "table_data": output_data,
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


def _download(self,bucket,file,download_path):
    client=boto3.resource('s3').meta.client
    return client.download_file(bucket,file,download_path)


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


