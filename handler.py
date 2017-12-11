from __future__ import print_function
import json
import imageio as io
from helpers.meanshift import MShift
import boto3
from datetime import datetime
import logger
import env


#
#   PRE-MAIN
#
if __name__ == "__main__":
    import local_env
    local_env.export('dev')




TABLE_NAME=env.get('table')
BUCKET_NAME=env.get('bucket',default=None)
RUN_DATE_STR=datetime.now().strftime("%Y%m%d")
TIMESTAMP_STR=datetime.now().strftime("%Y%m%d::%H:%M:%S")
REQUEST_DICT={
    'z': env.int('zoom'),
    'start': env.int('start_date'),
    'end': int(RUN_DATE_STR),
    'timestamp': TIMESTAMP_STR,
    'width': env.int('width'),
    'intensity_threshold': env.int('intensity_threshold'),
    'iterations': env.int('intensity_threshold'),
    'weight_by_intensity': env.bool('weight_by_intensity'),
    'min_count': env.int('min_count'),
    'downsample': env.int('downsample'),
    'url': env.get('url',default=None)
}


#
# AWS CLIENTS
#
s3 = boto3.client('s3')
table = boto3.resource('dynamodb').Table(TABLE_NAME)


#
# PUBLIC METHODS
#
def meanshift(event, context):
    file, data=_parse_request(event)
    logger.out("DATA {}\n\n".format(data))
    if False:
        print("TODO: load from remote URL")
        # download_path=_download_data(file,event.get('bucket',BUCKET_NAME))
        # input_data,clusters=cluster_data(download_path,data)
    else:
        bucket=event.get('bucket',BUCKET_NAME)
        print("DOWNLOAD FROM S3: {}/{}".format(bucket,file))
        download_path=_download_data(file,bucket)
        input_data,clusters=cluster_data(download_path,data)

    data['input_data']=input_data
    data['clusters']=clusters
    data['nb_clusters']=len(clusters)
    save_out(data)
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
# INTERNAL METHODS
#
def _process_glad(start,end,data):
    # mask by date range
    # return data
    pass


def _parse_request(request):
    request_dict=dict(REQUEST_DICT,**request)
    file=_get_file_path(request_dict)
    request_dict['file']=file
    request_dict['date_range']='{}-{}'.format(
            request_dict['start'],
            request_dict['end'],
        )
    return file, request_dict


def _download_data(file,bucket):
    download_path='/tmp/{}'.format(file)
    s3.download_file(bucket,file,download_path)
    return download_path


def _get_file_path(request):
    name=request.get('file')
    if not name:
        name='{}/{}/{}'.format(
                request.get('z'),
                request.get('x'),
                request.get('y')
            )
    base=REQUEST_DICT.get('tile_root')
    if base:
        name='{}{}'.format(base,name)
    return '{}.png'.format(name)    


def cluster_data(image_path,params):
    im=io.imread(image_path)
    ms=MShift(im,**params)
    input_data=ms.input_data()
    clusters=ms.clusters()
    return input_data.tolist(), clusters.tolist()


def save_out(data):
    table.put_item(Item=data)
    return data




#
#   POST-MAIN
#
if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description='CLUSTER LOCAL')
    parser.add_argument('data',help='json string')
    args=parser.parse_args()
    logger.out("\nRUN CLUSTER:\t{}".format(args.data))
    logger.out(meanshift(json.loads(args.data),None))


