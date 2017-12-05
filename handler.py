from __future__ import print_function
import json
import imageio as io
import numpy as np


#
#   CONFIG
#
TABLE_NAME='LambdaTest'
BUCKET_NAME='lambda-cluster-dev'
INDICES=np.indices((256,256))
THRESH=100
MIN_COUNT=1

#
#   GLOBAL HACK
#
data=None

import boto3
s3 = boto3.client('s3')
table = boto3.resource('dynamodb').Table(TABLE_NAME)


#
# PUBLIC METHODS
#
def resize(event, context):
    bucket=event.get('bucket') or BUCKET_NAME
    resize_name=event.get('resize') or '{}-resize'.format(event['file'])
    file='{}.png'.format(event['file'])
    resize_file='{}.png'.format(resize_name)
    download_path='/tmp/{}'.format(file)
    upload_path='/tmp/{}'.format(resize_file)


    s3.download_file(bucket,file,download_path)
    out,shape=resize_image(download_path,upload_path)

    item_json=save_out(file,resize_file,out)

    body = {
        "event": "{}".format(event),
        "message": "RESIZE IMAGE {}".format(len(out)),
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
def resize_image(image_path, upload_path, factors=(4,4)):
    im=io.imread(image_path)
    im=shiftdata(im)
    out=unq(im)
    return out.tolist(), out.shape


def save_out(file,resize_file,out):
    item_json={
            'input': file,
            'output': resize_file,
            'points': out
        }
    table.put_item(Item=item_json)
    return item_json


""" MEAN SHIFT START """
def shiftdata(img):
    global data
    data=img
    ijimage=np.apply_along_axis(ijdata,0,INDICES)
    ij_rows=ijimage.reshape(3,-1)
    zeros=np.all(np.equal(ij_rows, 0),axis=0)
    ijvals=ij_rows[:,~zeros]
    return ijvals


def ijdata(idx):
    global data
    i=idx[0]
    j=idx[1]
    val=data[i,j]
    if val>THRESH:
        return [i,j,val]
    else:
        return np.zeros(3)


def gaussian(d, bw):
    return np.exp(-0.5*((d/bw))**2) / (bw*math.sqrt(2*math.pi))


def meanshift(data,width=0.15,its=25):
    X = np.copy(data)
    for it in range(its):
        if not (it%5): print(it)
        for i, x in enumerate(X):
            dist = np.sqrt(((x-X)**2).sum(1))
            weight = gaussian(dist, width)
            X[i] = (np.expand_dims(weight,1)*X).sum(0) / weight.sum()
    return X


def unq(data):
    xu,count=np.unique(data,axis=0,return_counts=True)
    points=[]
    for t,cnt in zip(xu,count): 
        if cnt>=MIN_COUNT: 
            points.append(t.tolist())
    points=np.array(points)
    return points.astype(int)
""" MEAN SHIFT END """


#
#   MAIN
#
if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description='RESIZE LOCAL')
    parser.add_argument('data',help='json string')
    args=parser.parse_args()
    print("\nRUN RESIZE:\t{}".format(args.data))
    print(resize(json.loads(args.data),None))


