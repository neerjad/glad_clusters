from __future__ import print_function
import json
import imageio as io
from helpers.meanshift import MShift
import boto3


#
#   AWS CONFIG
#
TABLE_NAME='LambdaTest'
BUCKET_NAME='lambda-cluster-dev'
s3 = boto3.client('s3')
table = boto3.resource('dynamodb').Table(TABLE_NAME)



#
# PUBLIC METHODS
#
def meanshift(event, context):
    bucket=event.get('bucket') or BUCKET_NAME
    file='{}.png'.format(event['file'])
    download_path='/tmp/{}'.format(file)

    s3.download_file(bucket,file,download_path)
    out,shape=cluster_data(download_path)
    item_json=save_out(file,out)

    body = {
        "event": "{}".format(event),
        "message": "CLUSTER IMAGE {}".format(len(out)),
        "output": item_json,
        "shape": shape
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    return response


#
# INTERNAL METHODS
#
def cluster_data(image_path):
    im=io.imread(image_path)
    ms=MShift(im)
    out=ms.unq(ms.shiftdata())
    return out.tolist(), out.shape


def save_out(file,out):
    item_json={
            'input': file,
            'points': out
        }
    table.put_item(Item=item_json)
    return item_json


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


