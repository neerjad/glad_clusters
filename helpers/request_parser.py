from datetime import datetime
import env
import re

#
#   CONSTANTS
#
DEFAULT_START_DATE=20150101
DEFAULT_Z=5
DEFAULT_DOWNLOAD_FOLDER='/tmp'

#
#   REQUEST_PARSER
#
class RequestParser(object):

    PROPERTIES=[
        'z',
        'x',
        'y',
        'file_name',
        'download_folder',
        'url',
        'start_date',
        'end_date',
        'date_range',
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
        'z',
        'x',
        'y',
        'file_name',
        'start_date',
        'end_date',
        'date_range',
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
        request['file_name']=self._get_file_name(
            request.get('file_name'),
            request.get('z'),
            request.get('x'),
            request.get('y'))
        request['data_path']=self._get_data_path(
            request['file_name'],
            request.get('download_folder'),
            request.get('url'))
        print(request)
        return request


    def _get_file_name(self,file_name,z,x,y):
        if not file_name:
            file_name='{}/{}/{}'.format(z,x,y)
        if self.ext:
            if not re.search('.{}^'.format(self.ext), file_name):
                file_name='{}.{}'.format(file_name,self.ext)
        return file_name


    def _get_data_path(self,path,download_folder,url):
        if url:
            path='{}/{}'.format(url,path)
        elif download_folder:
            path='{}/{}'.format(download_folder,path)
        return path


    @staticmethod
    def _get_default_properties():
        now=datetime.now()
        return {
            'z': env.int('z',default=DEFAULT_Z),
            'start_date': env.int('start_date',default=DEFAULT_START_DATE),
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
            'bucket': env.get('bucket',default=None),
            'download_folder': env.get('download_folder',default=DEFAULT_DOWNLOAD_FOLDER)}




