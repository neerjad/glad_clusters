from __future__ import print_function
import json
import logging
import imageio as io
from clusters.meanshift import MShift
from clusters.request_parser import RequestParser
import clusters.processors as proc
from datetime import datetime, timedelta
import numpy as np

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


## Processor Functions

def glad_between_dates(
        data,
        start_date,
        end_date,
        return_days=True,
        return_intensity=False,
        image_type=None):
    """ filter glad image by dates

        Args:
            data<arr>: glad image
            start_date<str>: yyyy-mm-dd
            end_date<str>: yyyy-mm-dd
            return_days<bool|True>: if true return days-since band
            return_intensity<bool|False>: if true return intensity bands

        Returns:
            image with requested bands. note: if both return_days
            and return_intensity are false it will return an image
            with 1 if an alert exists between the dates, otherwise 0.

    """
    DATE_STR_FMT = '%Y-%m-%d'
    INT_DATE_FMT = '%Y%m%d'
    if image_type == "FORMA":
        GLAD_START_DATE = datetime.strptime('2012-01-01', DATE_STR_FMT)
    elif image_type == "GLAD":
        GLAD_START_DATE = datetime.strptime('2015-01-01', DATE_STR_FMT)
    else:
        GLAD_START_DATE = datetime.strptime('2015-01-01', DATE_STR_FMT)

    intensity, days = _get_intensity_days(data)
    is_between_dates = _days_are_between_dates(days, start_date, end_date)
    if return_days:
        if return_intensity:
            bands = [_between_dates(is_between_dates, intensity),
                     _between_dates(is_between_dates, days)]
            im = np.dstack(bands)
        else:
            im = _between_dates(is_between_dates, days)
    elif return_intensity:
        im = _between_dates(is_between_dates, intensity)
    else:
        im = is_between_dates.astype(int)
    return im


def date_for_days(days):
    date = (GLAD_START_DATE + timedelta(days=days))
    return int(date.strftime(INT_DATE_FMT))


def _between_dates(is_between_dates, im):
    return np.where(is_between_dates, im, 0)


def _days_are_between_dates(days, start_date, end_date):
    start_days = _days_since_glad_start(start_date)
    end_days = _days_since_glad_start(end_date)
    return np.logical_and(days >= start_days, days < end_days)


def _days_since_glad_start(date_str):
    date = datetime.strptime(date_str, DATE_STR_FMT)
    return (date - GLAD_START_DATE).days


def _get_intensity_days(data):
    confidence_intensity = data[:, :, 2]
    days = (255.0 * data[:, :, 0]) + data[:, :, 1]
    intensity = np.mod(confidence_intensity, 100.0) * 100.0 / 55
    return intensity, days


def meanshift(event, context, image_type=None):
    req = RequestParser(event)
    if req.is_not_valid():
        return _error(req, 'request not valid', 1)
    else:
        try:
            im_data = _im_data(req)
            if im_data is False:
                return _error(req, '{} not found'.format(req.data_path), 2)
            else:
                im_data = glad_between_dates(
                    im_data,
                    req.start_date,
                    req.end_date, image_type)

                mshift = MShift(
                    data=im_data,
                    width=req.width,
                    min_count=req.min_count,
                    iterations=req.iterations)
                output_data, nb_clusters = _output_data(req, mshift)
                if (nb_clusters > 0) or RETURN_EMPTY:
                    return output_data
                else:
                    return None
        except Exception as e:
            return _error(req, 'Exception: {}'.format(e), 3)


def _im_data(req):
    if not req.url: _download(req.bucket, req.file_name, req.data_path)
    try:
        return io.imread(req.data_path)
    except Exception as e:
        logger.warn(
            "\nfailed to read image ({}) -- {}".format(req.data_path, e))
        return False


def _output_data(req, mshift):
    data = req.data()
    data['data'] = mshift.clusters_data() or {}
    nb_clusters = data['data'].pop('nb_clusters', 0)
    data['nb_clusters'] = nb_clusters
    return data, nb_clusters


def _error(req, msg, trace_id):
    error = {'error': msg, 'error_trace': 'handler.{}'.format(trace_id)}
    error.update(req.data())
    return error


def _process_response(event, output_data):
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


def _download(self, bucket, file, download_path):
    client = boto3.resource('s3').meta.client
    return client.download_file(bucket, file, download_path)


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
