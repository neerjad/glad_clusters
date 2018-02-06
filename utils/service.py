import os
from time import sleep
import math
import itertools
import json
import boto3
from boto3.dynamodb.conditions import Attr
from boto3.session import Config
import numpy as np
import pandas as pd
import utils.multiprocess as mp
from pprint import pprint


DEFAULT_START_DATE='2015-01-01'
DEFAULT_END_DATE='2025-01-01'
DEFAULT_MIN_COUNT=25
DEFAULT_WIDTH=5
DEFAULT_ITERATIONS=25
DEFAULT_ZOOM=12
DELETE_RESPONSES=False
DEFAULT_TABLE=os.environ.get('table')
DEFAULT_BATCH_SIZE=100
LISTEN_WAIT=1.0

LAMBDA_FUNCTION_NAME='gfw-glad-clusters-v1-dev-meanshift'

DATAFRAME_COLUMNS=[
    'count',
    'area',
    'min_date',
    'max_date',
    'latitude',
    'longitude',
    'z','x','y','i','j',
    'file_name',
    'timestamp',
    'alerts',
    'input_data']


VIEW_COLUMNS=[
    'count',
    'area',
    'min_date',
    'max_date',
    'latitude',
    'longitude',
    'x','y',
    'timestamp']


ERROR_COLUMNS=[
    'z','x','y',
    'centroid_latitude',
    'centroid_longitude',
    'error',
    'error_trace']


BOTO3_CONFIG={
    'read_timeout': 600
}

WUP_SCALE=200
RUP_SCALE=200
WDOWN_SCALE=5
RDOWN_SCALE=5
MAX_PROCESSES=100

class ClusterService(object):

    @staticmethod
    def scale(
            write=None,
            read=None,
            wup=False,
            rup=False,
            up=False,
            down=False,
            table=None,
            listen=True,
            noisy=True,
            aws_response=False):
        """ update read/write capcity for table 

            Args:
                write<int>: Write Capacity **MUST BE USED WITH 'read'**
                read<int>: Read Capacity **MUST BE USED WITH 'write'**
                wup<bool>: Scale-Up Write
                rup<bool>: Scale-Up Read
                up<bool>: Scale-Up Write and Read
                down<bool>: Scale-Down Write and Read
                table<str>: TableName defaults to eviron['table']
                listen<bool>: Wait for status to be "Active"
                noisy<bool>: Print "..." while waiting
        """
        if not table: table=os.environ['table']
        config={}
        if write and read:
            config['ReadCapacityUnits']=read
            config['WriteCapacityUnits']=write
        elif wup:
            config['ReadCapacityUnits']=RDOWN_SCALE
            config['WriteCapacityUnits']=WUP_SCALE
        elif rup:
            config['ReadCapacityUnits']=RUP_SCALE
            config['WriteCapacityUnits']=WDOWN_SCALE
        elif up:
            config['ReadCapacityUnits']=RUP_SCALE
            config['WriteCapacityUnits']=WUP_SCALE
        elif down:
            config['ReadCapacityUnits']=RDOWN_SCALE
            config['WriteCapacityUnits']=WDOWN_SCALE
        db_client=boto3.client('dynamodb')
        try:
            aws_resp=db_client.update_table(
                    TableName=table,
                    ProvisionedThroughput=config)
            if aws_response:
                return aws_resp
            elif listen:
                return ClusterService.listen(table,noisy)
            else:
                return ClusterService.status(table)
        except Exception as e:
            return "WARNING: {}".format(e)


    @staticmethod
    def listen(table=None,noisy=True):
        """ Wait for status "Active"

            Args:
                table<str>: TableName defaults to eviron['table']
                noisy<bool>: Print "..." while waiting
        """
        while ClusterService.status(table)!=u'ACTIVE':
            if noisy: print('...')
            sleep(LISTEN_WAIT)
        return True


    @staticmethod
    def status(table=None):
        """ get table status 

            Args:
                table<str>: TableName defaults to eviron['table']
        """
        if not table: table=os.environ['table']
        db=boto3.resource('dynamodb')
        return db.Table(table).table_status


    #
    #  PUBLC METHODS
    #    
    def __init__(self,
            bounds=None,
            tile_bounds=None,
            lat=None,
            lon=None,
            x=None,
            y=None,
            start_date=DEFAULT_START_DATE,
            end_date=DEFAULT_END_DATE,
            min_count=DEFAULT_MIN_COUNT,
            width=DEFAULT_WIDTH,
            iterations=DEFAULT_ITERATIONS,
            z=DEFAULT_ZOOM,
            table=DEFAULT_TABLE):
        self._init_properties()
        self.start_date=DEFAULT_START_DATE
        self.end_date=DEFAULT_END_DATE
        self.min_count=min_count
        self.width=width
        self.iterations=iterations
        self.z=z
        self._N=(2**self.z)
        self.table=table or os.environ['table']
        self._set_tile_bounds(bounds,tile_bounds,lat,lon,x,y)
        


    def fetch(self,key=None,query=None,**kwargs):
        """ fetch clusters from dynamodb 
        """
        db=boto3.resource('dynamodb')
        table=db.Table(self.table)
        if key:
            self.responses=[table.get_item(Key=key)]
        else:
            if query or kwargs:
                filter_expression=self._db_filter(query or kwargs)
            else:
                filter_expression=self._build_filter()
            rows=table.scan(FilterExpression=filter_expression)
            self.responses=rows.get('Items')


    def batch_run(self,batch_size=DEFAULT_BATCH_SIZE,max_processes=MAX_PROCESSES):
        """ find clusters on tiles
        
            NOTE: if (self.x and self.y): 
                    pass directly to _run_tile
                  else:
                    use multiprocessing
        """
        self.lambda_client=boto3.client('lambda',config=Config(**BOTO3_CONFIG))
        self.responses=[]
        xys=list(itertools.product(
            range(self.x_min,self.x_max+1),
            range(self.y_min,self.y_max+1)))
        self._nb_batches=math.ceil(len(xys)/batch_size)
        self._batch_index=0
        while self._batch_index<self._nb_batches:
            start=self._batch_index*batch_size
            end=start+batch_size
            self.responses+=mp.map_with_threadpool(
                self._run_tile,
                list(xys[start:end]),
                max_processes=max_processes)
            self._batch_index+=1
            print(self._batch_index,start,end)



    def run(self,max_processes=MAX_PROCESSES):
        """ find clusters on tiles
        
            NOTE: if (self.x and self.y): 
                    pass directly to _run_tile
                  else:
                    use multiprocessing
        """
        self.lambda_client=boto3.client('lambda',config=Config(**BOTO3_CONFIG))
        if (self.x and self.y):
            self.responses=[self._run_tile()]
        else:
            xys=itertools.product(
                range(self.x_min,self.x_max+1),
                range(self.y_min,self.y_max+1))
            self.responses=mp.map_with_threadpool(
                self._run_tile,
                list(xys),
                max_processes=max_processes)

    
    def request_size(self):
        return (self.x_max-self.x_min+1)*(self.y_max-self.y_min+1)


    def dataframe(self):
        """ return data frame of clusters data
            
            NOTE: if DELETE_RESPONSES is True
                  responses json will be removed
        """
        if  self._dataframe is None:
            self._process_dataframes()
        return self._dataframe


    def view(self):
        """ return only VIEW_COLUMNS of .dataframe()
            * excludes data arrays, i and j, ...
        """
        return self.dataframe()[VIEW_COLUMNS]


    def errors(self):
        """ return data frame of clusters data
            
            NOTE: if DELETE_RESPONSES is True
                  responses json will be removed
        """
        if  self._error_dataframe is None:
            self._process_dataframes()
        return self._error_dataframe


    def cluster(self,
            row_id=None,
            lat=None,lon=None,
            z=None,x=None,y=None,i=None,j=None,
            timestamp=None,
            ascending=False,
            as_view=True):
        """ fetch cluster data

            Convince method for selecting row of dataframe
            
            Args:
                row_id<int>: dataframe index for cluster
                lat,lon<floats>: latitude,longitude for cluster
                z,x,y,i,j<ints>: tile/pixel location for cluster
                timestamp<str>: timestamp for cluster (consider using row_id)
                ascending<bool>: 
                    if true sort by ascending time and grab first matching row
                as_view:
                    if true return only VIEW_COLUMNS. 
                    else include all columns (including input/alerts data)
        """
        if self._not_none([row_id]):
            row=self.dataframe().iloc[row_id]
        else:
            test=True
            if self._not_none([lat,lon]):
                test=test & (
                    (self.dataframe().latitude==lat) & 
                    (self.dataframe().longitude==lon))
            elif self._not_none([x,y,z]):
                test=test & (
                    (self.dataframe().z==z) & 
                    (self.dataframe().x==x) & 
                    (self.dataframe().y==y))
            if timestamp:
                test=test & (self.dataframe().timestamp==timestamp)
            rows=self.dataframe()[test]
            if ascending: rows.sort_values('timestamp',inplace=True)
            row=rows.iloc[0]
        if as_view:
            return row[VIEW_COLUMNS]
        else:
            return row



    #
    #  INTERNAL METHODS
    #
    def _init_properties(self):
        self.x=None
        self.y=None
        self._dataframe=None
        self._error_dataframe=None


    def _db_filter(self,query):
        keys=query.keys()
        key0=keys.pop()
        db_filter=Attr(key0).eq(query[key0])
        for key in keys:
            db_filter&=Attr(key).eq(query[key])
        return db_filter


    def _build_filter(self):
        db_filter=Attr('z').eq(self.z)
        if self.start_date:
            db_filter &= Attr('start_date').gte(self.start_date)
        if self.end_date:
            db_filter &= Attr('end_date').lte(self.end_date)
        if self.min_count:
            db_filter &= Attr('min_count').eq(self.min_count)
        if self.width:
            db_filter &= Attr('width').eq(self.width)
        if self.iterations:
            db_filter &= Attr('iterations').eq(self.iterations)
        if self.x and self.y:
            db_filter &= Attr('x').eq(self.x)
            db_filter &= Attr('y').eq(self.y)
        if self.x_min and self.x_max:
            db_filter &= Attr('x').gte(self.x_min)
            db_filter &= Attr('x').lte(self.x_max)
        if self.y_min and self.y_max:
            db_filter &= Attr('y').gte(self.y_min)
            db_filter &= Attr('y').lte(self.y_max)
        return db_filter


    def _request_data(self,x,y,as_dict=False):
        data={
            "z":self.z,
            "x":x,
            "y":y,
            "start_date":self.start_date,
            "end_date":self.end_date,
            "min_count":self.min_count,
            "width":self.width,
            "iterations":self.iterations }
        if as_dict:
            return data
        else:
            return json.dumps(data)

    
    def _set_tile_bounds(self,bounds,tile_bounds,lat,lon,x,y):
        """
            NOTE: if a single pair (x,y) or (lat,lon) the x,y-values 
            will be set for the find_by_tile method.
        """
        if bounds:
            tile_bounds=[self._latlon_to_xy(*latlon) for latlon in bounds]
        elif (lat and lon):
            self.x,self.y=self._latlon_to_xy(lat,lon)
            tile_bounds=[[self.x,self.y],[self.x,self.y]]
        elif (x and y):
            self.x=int(x)
            self.y=int(y)
            tile_bounds=[[self.x,self.y],[self.x,self.y]]
        tile_bounds=np.array(tile_bounds).astype(int)
        self.x_min,self.y_min=tile_bounds.min(axis=0)
        self.x_max,self.y_max=tile_bounds.max(axis=0)
            
            
    def _latlon_to_xy(self,lat,lon):
        lat_rad=math.radians(lat)
        x=self._N*(lon+180.0)/360
        y=self._N*(1.0-math.log(math.tan(lat_rad)+(1/math.cos(lat_rad)))/math.pi)/2.0
        return int(x),int(y)
    
    
    def _lat(self,z,x,y,i,j):
        lat_rad=math.atan(math.sinh(math.pi*(1-(2*(y+(j/255.0))/self._N))))
        lat=(lat_rad*180.0)/math.pi
        return lat


    def _lon(self,z,x,y,i,j):
        lon=(360.0/self._N)*(x+(i/255.0))-180.0
        return lon


    def _process_response(self,x,y,response):
        if response:
            payload=json.loads(response.get('Payload',{}).read())
            if payload:
                response=self._request_data(x,y,as_dict=True)
                response.update(payload)
                return payload
        return None


    def _run_tile(self,location=None,x=None,y=None):
        """ find clusters on tile
        
            NOTE: if no args are passed it will attempt to use 
                  the x,y (or lat,lon) passed in the constructor
        
            Args:
                location<tuple>: tile-xy value (x,y)
                x<int>: tile x value
                y<int>: tile y value
        """
        if location: x,y=location
        if not (x and y):
            x=self.x
            y=self.y
        if (x and y):
            try:
                response=self.lambda_client.invoke(
                    FunctionName=LAMBDA_FUNCTION_NAME,
                    InvocationType='RequestResponse',
                    LogType='Tail',
                    Payload=self._request_data(x,y))
                return self._process_response(x,y,response)
            except Exception as e:
                error_data=self._request_data(x,y,as_dict=True)
                error_data['data']={}
                error_data['error']="{}".format(e)
                error_data['error_trace']="service"
                return error_data


    def _process_dataframes(self):
        rows,error_rows=self._dataframes_rows()
        self._dataframe=pd.DataFrame(
            rows, 
            columns=DATAFRAME_COLUMNS)
        self._error_dataframe=pd.DataFrame(
            error_rows, 
            columns=ERROR_COLUMNS)
        self._dataframe.sort_values(
            'timestamp',
            ascending=False,
            inplace=True)
        self._dataframe.reset_index(inplace=True)
        self._error_dataframe.reset_index(inplace=True)
        if DELETE_RESPONSES: self.responses=None


    def _dataframes_rows(self):
        rows=[]
        error_rows=[]
        for response in self.responses:
            if response:
                error=response.get('error') or response.get('errorMessage')
                if error:
                    error_rows.append(self._error_row(error,response))
                else:
                    rows+=self._response_rows(response)
        return rows,error_rows


    def _response_rows(self,response):
        rrows=[]
        z=int(response.get('z'))
        x=int(response.get('x'))
        y=int(response.get('y'))
        for cluster in response.get('data',{}).get('clusters',[]):
            i=int(cluster.get('i'))
            j=int(cluster.get('j'))
            rrows.append([
                    int(cluster.get('count')),
                    int(cluster.get('area')),
                    cluster.get('min_date'),
                    cluster.get('max_date'),
                    self._lat(z,x,y,i,j),
                    self._lon(z,x,y,i,j),
                    z,x,y,i,j,
                    response['file_name'],
                    response['timestamp'],
                    np.array(cluster.get('alerts')).astype(int),
                    np.array(response['data']['input_data']).astype(int)])
        return rrows


    def _error_row(self,error,response):
        error_trace=response.get('error_trace','lambda')
        z=response.get('z')
        x=response.get('x')
        y=response.get('y')
        if (z and x and y):
            lat=self._lat(int(z),int(x),int(y),128,128)
            lon=self._lon(int(z),int(x),int(y),128,128)
        else:
            lat,lon=None,None
        return [z,x,y,lat,lon,error,error_trace]


    def _not_none(self,values):
        test=[ (val is not None) for val in values ]
        return np.prod(test).astype(bool)






