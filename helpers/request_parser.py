from datetime import datetime
import env

#
#   SETUP
#
GLAD_START_DATE_STR='20150101'
BUCKET_NAME=env.get('bucket',default=None)
RUN_DATE_INT=int(datetime.now().strftime("%Y%m%d"))
TIMESTAMP_STR=datetime.now().strftime("%Y%m%d::%H:%M:%S")
REQUEST_DICT={
    'z': env.int('zoom'),
    'start': env.int('start_date',default=GLAD_START_DATE_STR),
    'end': RUN_DATE_INT,
    'timestamp': TIMESTAMP_STR,
    'width': env.int('width'),
    'intensity_threshold': env.int('intensity_threshold'),
    'iterations': env.int('intensity_threshold'),
    'weight_by_intensity': env.bool('weight_by_intensity'),
    'min_count': env.int('min_count'),
    'downsample': env.int('downsample'),
    'url': env.get('url',default=None),
    'table_name': env.get('table')
}




class RequestParser(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,request_dict):
        self.request_dict=request_dict

def _parse_request(request):
    request_dict=dict(REQUEST_DICT,**request)
    file=_get_file_path(request_dict)
    request_dict['file']=file
    request_dict['date_range']='{}-{}'.format(
            request_dict['start'],
            request_dict['end'],
        )
    return file, request_dict.get('url'), request_dict



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

