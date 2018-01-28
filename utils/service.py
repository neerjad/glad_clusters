import math
import itertools
import json
import boto3
import numpy as np
import pandas as pd
import utils.multiprocess as mp


DEFAULT_START_DATE='2015-01-01'
DEFAULT_END_DATE='2025-01-01'
DEFAULT_MIN_COUNT=25
DEFAULT_WIDTH=5
DEFAULT_ITERATIONS=25
DEFAULT_ZOOM=12
LAMBDA_FUNCTION_NAME='gfw-glad-clusters-v1-dev-meanshift'
DATAFRAME_COLUMNS=[
    'timestamp',
    'count',
    'area',
    'min_date',
    'max_date',
    'latitude',
    'longitude',
    'z','x','y','i','j',
    'alerts',
    'input_data']


VIEW_COLUMNS=[
    'timestamp',
    'count',
    'area',
    'min_date',
    'max_date',
    'latitude',
    'longitude',
    'x','y']


class ClusterService(object):
    
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
            z=DEFAULT_ZOOM):
        self._init_properties()
        self.start_date=DEFAULT_START_DATE
        self.end_date=DEFAULT_END_DATE
        self.min_count=min_count
        self.width=width
        self.iterations=iterations
        self.z=z
        self._N=(2**self.z)
        self._set_tile_bounds(bounds,tile_bounds,lat,lon,x,y)
        self.client=boto3.client('lambda')
        
    
    def run(self):
        """ find clusters on tiles
        
            NOTE: if (self.x and self.y): 
                    pass directly to run_tile
                  else:
                    use multiprocessing
        """
        if (self.x and self.y):
            self.responses=[self.run_tile()]
        else:
            xys=itertools.product(
                range(self.x_min,self.x_max+1),
                range(self.y_min,self.y_max+1))
            self.responses=mp.map_with_threadpool(self.run_tile,list(xys))


    def run_tile(self,location=None,x=None,y=None):
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
            response=self.client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=self._request_data(x,y))
            return self._process_response(response)

    
    def dataframe(self):
        if  self._dataframe is None:
            self._dataframe=pd.DataFrame(
                self._dataframe_rows(),
                columns=DATAFRAME_COLUMNS)
            self._dataframe.sort_values(
                'timestamp',
                ascending=False,
                inplace=True)
        return self._dataframe


    def view(self):
        return self.dataframe()[VIEW_COLUMNS]


    def clusters(self,start=None,end=None):
        if start or end:
            responses=self.responses[start:end]
        elif row_ids is not None:
            responses=self.responses[start:end]
        else:
            responses=self.responses
            clusters=[]
            for response in responses:
                for cluster in response['data']['clusters']:
                    cinfo=cluster.copy()
                    cinfo['z']=response['z']
                    cinfo['x']=response['x']
                    cinfo['y']=response['y']
                    clusters.append(cinfo)
        return clusters  


    def tile(self,
            row_id=None,
            lat=None, lon=None,
            z=None, x=None, y=None):
        """ GET CLUSTER FOR PASSED INFO """
        if self._not_none([row_id]):
            row=self.dataframe().iloc[row_id]
        elif self._not_none([lat,lon]):
            test=(
                (self.dataframe().latitude==lat) & 
                (self.dataframe().longitude==lon))
            row=self.dataframe()[test].iloc[0]
        elif self._not_none([x,y,z]):
            test=(
                (self.dataframe().z==z) & 
                (self.dataframe().x==x) & 
                (self.dataframe().y==y))
            row=self.dataframe()[test].iloc[0]
        query={
            'z': row.z,
            'x': row.x,
            'y': row.y }
        return self._query_responses(query)



    def _dataframe_rows(self):
        dfrows=[]
        for response in self.responses:
            t=response.get('timestamp')
            z=response.get('z')
            x=response.get('x')
            y=response.get('y')
            for cluster in response['data']['clusters']:
                i=cluster.get('i')
                j=cluster.get('j')
                dfrows.append([
                        t,
                        cluster.get('count'),
                        cluster.get('area'),
                        cluster.get('min_date'),
                        cluster.get('max_date'),
                        self._lat(z,x,y,i,j),
                        self._lon(z,x,y,i,j),
                        z,x,y,i,j,
                        cluster.get('alerts'),
                        response['data']['input_data']])
                        
        return dfrows


    def _init_properties(self):
        self.x=None
        self.y=None
        self._dataframe=None
        self._clusters=None

    
    def _request_data(self,x,y):
        return json.dumps({
            "z":self.z,
            "x":x,
            "y":y,
            "start_date":self.start_date,
            "end_date":self.end_date,
            "min_count":self.min_count,
            "width":self.width,
            "iterations":self.iterations })

    
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
            self.x=x
            self.y=y
            tile_bounds=[[self.x,self.y],[self.x,self.y]]
        tile_bounds=np.array(tile_bounds)
        self.x_min,self.y_min=tile_bounds.min(axis=0)
        self.x_max,self.y_max=tile_bounds.max(axis=0)
            
            
    def _latlon_to_xy(self,lat,lon):
        lat_rad=math.radians(lat)
        x=self._N*(lon+180.0)/360
        y=self._N*(1.0-math.log(math.tan(lat_rad)+(1/math.cos(lat_rad)))/math.pi)/2.0
        return int(x),int(y)
    
    
    def _lat(self,z,x,y,i,j):
        """ TODO CONVERT zxyij to LAT """
        return -999


    def _lon(self,z,x,y,i,j):
        """ TODO CONVERT zxyij to LAT """
        return -999


    def _process_response(self,response):
        return json.loads(response['Payload'].read())


    def _query_responses(self,query):
        return next(
            (r for r in self.responses if self._test_response(query,r)),
            None)


    def _not_none(self,values):
        test=[ (val is not None) for val in values ]
        return np.prod(test).astype(bool)


    def _test_response(self,query,response):
        test=[ query[key]==response[key] for key in query.keys()]
        return np.prod(test).astype(bool)






