from __future__ import print_function
import json
import imageio as io
from helpers.meanshift import MShift
import boto3
import datetime


#
#   AWS CONFIG
#
TABLE_NAME='MShiftDEV'
BUCKET_NAME='lambda-cluster-dev'
DEFAULT_ZOOM=4
DEFAULT_WIDTH=15
DEFAULT_ITERATIONS=25
DEFAULT_INTENSITY_THRESHOLD=100
DEFAULT_MIN_COUNT=4
DEFALUT_WEIGHT_INTENSITY=False
DEFALUT_DOWNSAMPLE=False

s3 = boto3.client('s3')
table = boto3.resource('dynamodb').Table(TABLE_NAME)
GLAD_START_DATE='20150101'
RUN_DATE_STR=datetime.datetime.now().strftime("%Y%m%d")
TIMESTAMP_STR=datetime.datetime.now().strftime("%Y%m%d::%H:%M:%S")
REQUEST_DICT={
    'z': DEFAULT_ZOOM,
    'start': int(GLAD_START_DATE),
    'end': int(RUN_DATE_STR),
    'timestamp': TIMESTAMP_STR,
    'width':int(DEFAULT_WIDTH),
    'intensity_threshold':DEFAULT_INTENSITY_THRESHOLD,
    'iterations':DEFAULT_ITERATIONS,
    'weight_by_intensity': DEFALUT_WEIGHT_INTENSITY,
    'min_count':DEFAULT_MIN_COUNT,
    'downsample':DEFALUT_DOWNSAMPLE
}

#
# PUBLIC METHODS
#
def meanshift(event, context):
    file, data=_parse_request(event)
    download_path=_download_data(file,event.get('bucket',BUCKET_NAME))
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
    return '{}.png'.format(name)


def cluster_data(image_path,params):
    im=io.imread(image_path)
    ms=MShift(im,params)
    input_data=ms.input_data()
    clusters=ms.clusters()
    return input_data.tolist(), clusters.tolist()


def save_out(data):
    table.put_item(Item=data)
    return data


#
#   MAIN
#
if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description='CLUSTER LOCAL')
    parser.add_argument('data',help='json string')
    args=parser.parse_args()
    print("\nRUN CLUSTER:\t{}".format(args.data))
    print(meanshift(json.loads(args.data),None))


