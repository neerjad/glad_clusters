import math
import itertools
import json
import boto3
import numpy as np
import utils.multiprocess as mp


DEFAULT_START_DATE='2015-01-01'
DEFAULT_END_DATE='2025-01-01'
DEFAULT_MIN_COUNT=5
DEFAULT_WIDTH=5
DEFAULT_ITERATIONS=25
DEFAULT_ZOOM=12


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
            return self.run_tile()
        else:
            xys=itertools.product(
                range(self.x_min,self.x_max+1),
                range(self.y_min,self.y_max+1))
            return mp.map_with_threadpool(self.run_tile,list(xys))


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
                FunctionName='gfw-glad-clusters-dev-meanshift',
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=self._request_data(x,y))
            return self._process_response(x,y,response)

    
    def _init_properties(self):
        self.x=None
        self.y=None

    
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
    
    
    def _process_response(self,x,y,response):
        clusters=self._response_table_data(response).get("clusters")
        return {
            'x':x,
            'y':y,
            'clusters':clusters}
    
    
    def _response_table_data(self,response):
        r=json.loads(response['Payload'].read())
        r=r.get("body")
        if r:
            r=json.loads(r).get("table_data")
            if r:
                return r
        return {}

