from datetime import datetime
import env

#
#   CONSTANTS
#
GLAD_START_DATE_STR='20150101'


#
#   REQUEST_PARSER
#
class RequestParser(object):

    PROPERTIES=[
        'z',
        'x',
        'y',
        'file_name',
        'url',
        'data_root',
        'date_range',
        'zoom',
        'start_date',
        'end_date',
        'timestamp',
        'width',
        'intensity_threshold',
        'iterations',
        'weight_by_intensity',
        'min_count',
        'downsample',
        'table_name',
        'bucket',
        'data_path']


    DATA_PROPERTIES=[
        'data_path',
        'date_range',
        'z',
        'x',
        'y',
        'start_date',
        'end_date',
        'timestamp',
        'width',
        'intensity_threshold',
        'iterations',
        'weight_by_intensity',
        'min_count',
        'downsample']


    #
    # PUBLIC METHODS
    #
    def __init__(self,request,ext='png'):
        self.ext=ext
        self.request=self._process_request(request)
        self._update_properties()


    def data(self):
        return {prop: getattr(self,prop) for prop in self.DATA_PROPERTIES}


    #
    # INTERNAL METHODS
    #
    def _update_properties(self):
        for prop in self.PROPERTIES:
            setattr(self,prop,self.request.get(prop))


    def _process_request(self,request):
        request=dict(RequestParser._get_default_properties(),**request)
        request['date_range']='{}-{}'.format(
                request['start_date'],
                request['end_date'],
            )
        request['data_path']=self._get_data_path(request)
        return request


    def _get_data_path(self,request):
        path=request.get('file_name')
        if not path:
            path='{}/{}/{}'.format(
                    request.get('z'),
                    request.get('x'),
                    request.get('y')
                )
        data_root=request.get('data_root')
        url=request.get('url')
        if data_root:
            path='{}/{}'.format(data_root,path)
        if url:
            path='{}/{}'.format(url,path)
        if self.ext:
            path='{}.{}'.format(path,self.ext)
        return path


    @staticmethod
    def _get_default_properties():
        now=datetime.now()
        return {
            'z': env.int('zoom'),
            'start_date': env.int('start_date',default=GLAD_START_DATE_STR),
            'end_date': env.int('end_date',default=now.strftime("%Y%m%d")),
            'timestamp': now.strftime("%Y%m%d::%H:%M:%S"),
            'width': env.int('width'),
            'intensity_threshold': env.int('intensity_threshold'),
            'iterations': env.int('intensity_threshold'),
            'weight_by_intensity': env.bool('weight_by_intensity'),
            'min_count': env.int('min_count'),
            'downsample': env.int('downsample'),
            'url': env.get('url',default=None),
            'table_name': env.get('table'),
            'bucket': env.get('bucket',default=None)}




