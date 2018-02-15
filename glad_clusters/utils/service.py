from __future__ import print_function
import os
from datetime import datetime
import math
import itertools
import json
import boto3
from boto3.session import Config
import numpy as np
import pandas as pd
import glad_clusters.utils.multiprocess as mp
import psycopg2
from glad_clusters.clusters.convex_hull import ConvexHull
import inspect
import argparse
from argparse import ArgumentParser
import copy


CSV_ACL='public-read'
S3_URL_TMPL='https://s3-{}.amazonaws.com'
DEFAULT_REGION='us-west-2'
DEFAULT_START_DATE='2015-01-01'
DEFAULT_END_DATE=datetime.now().strftime("%Y-%m-%d")
DEFAULT_MIN_COUNT=25
DEFAULT_WIDTH=5
DEFAULT_ITERATIONS=25
DEFAULT_ZOOM=12
DELETE_RESPONSES=True
DEFAULT_BUCKET='gfw-clusters-test'
LAMBDA_FUNCTION_NAME='gfw-glad-clusters-v1-dev-meanshift'
DEFAULT_CSV_IDENT='clusters'
CSV_NAME_TMPL="{}_{}%{}_{}%{}%{}%{}_{}%{}%{}%{}"
CONVERTERS={ "alerts" :lambda r: np.array(json.loads(r)) }


DATAFRAME_COLUMNS=[
    'count',
    'area',
    'min_date',
    'max_date',
    'longitude',
    'latitude',
    'z','x','y','i','j',
    'file_name',
    'timestamp',
    'alerts']


VIEW_COLUMNS=[
    'count',
    'area',
    'min_date',
    'max_date',
    'longitude',
    'latitude',
    'x','y',
    'timestamp']


ERROR_COLUMNS=[
    'z','x','y',
    'centroid_longitude',
    'centroid_latitude',
    'error',
    'error_trace']

BOTO3_CONFIG={
    'read_timeout': 600,
    'region_name': 'us-east-1'
}

MAX_PROCESSES=200

class ClusterService(object):
    """ ClusterService:
        
        Creates service for running cluster algorithm and/or viewing 
        the resulting cluster data.

        Args:
            Use one of the following to select tiles to run:

                bounds<list>: tiles-lonlat bounding box
                tile_bounds<list>: tiles-xy bounding box
                lat,lon<int,int>: latitude,longitude used to run a single tile
                x,y<int,int>: tile-xy used to run a single tile

            Other run arguments:

                start_date<str>: 'yyyy-mm-dd'
                end_date<str>: 'yyyy-mm-dd'
                min_count<int>: minimum number of alerts in a cluster
                width<int>: gaussian width in cluster algorithm
                iterations<int>: number of times to iterate when finding clusters
                z<int>: tile-zoom
                bucket<str>: aws-bucket used for saving csv file

            Preloaded dataframe args:

                NOTE: Consider using ClusterService.read() rather than loading the
                dataframes directly.

                dataframe<pandas.dataframe>,
                errors_dataframe<pandas.dataframe>
    """
    @staticmethod
    def get_dataframes(filename,
            local=False,
            region=DEFAULT_REGION,
            bucket=DEFAULT_BUCKET,
            url_base=None,
            errors=True):
        """ get dataframes from csv

            Args:
                filename<str>: name/path of csv without '.csv' extension 
                local<bool[False]>: if true read from local file else read from s3 file
                region<str>: aws-region required if not local and not url_base
                bucket<str>: aws-bucket required if not local
                url_base<str>: aws-url-root for bucket
                errors<bool[True]>: if true include errors-csv
        """
        if local:
            dfpath='{}.csv'.format(filename)
            if errors: edfpath='{}.errors.csv'.format(filename)
        else:
            dfpath,edfpath=ClusterService.get_urls(filename,region,bucket,url_base,errors)
        df=pd.read_csv(dfpath,converters=CONVERTERS)
        if errors:
            try:
                edf=pd.read_csv(edfpath)
            except:
                edf=None
        else: edf=None
        return df, edf



    @staticmethod
    def get_urls(filename,
            region=DEFAULT_REGION,
            bucket=DEFAULT_BUCKET,
            url_base=None,
            errors=True):
        """ get urls for dataframe csvs

            Args:
                filename<str>: name/path of csv without '.csv' extension 
                region<str>: aws-region required if not local and not url_base
                bucket<str>: aws-bucket required if not local
                url_base<str>: aws-url-root for bucket
                errors<bool[True]>: if true include errors-csv-url
        """
        if not url_base: url_base=S3_URL_TMPL.format(region)
        url_base="{}/{}".format(url_base,bucket)
        dfpath='{}/{}.csv'.format(url_base,filename)
        if errors:
            edfpath='{}/{}.errors.csv'.format(url_base,filename)
            return dfpath, edfpath
        else:
            return dfpath


    @staticmethod
    def read_csv(filename,
            local=False,
            region=DEFAULT_REGION,
            bucket=DEFAULT_BUCKET,
            url_base=None,
            errors=True):
        """ init service from csv

            Args:
                filename<str>: name/path of csv without '.csv' extension 
                local<bool[False]>: if true read from local file else read from s3 file
                region<str>: aws-region required if not local and not url_base
                bucket<str>: aws-bucket required if not local
                url_base<str>: aws-url-root for bucket
                errors<bool[True]>: if true include errors-csv
        """
        df,edf=ClusterService.get_dataframes(
            filename,
            local,
            region,
            bucket,
            url_base,
            errors)
        run_params=ClusterService.run_params(df)
        return ClusterService(
                dataframe=df,
                errors_dataframe=edf,
                **run_params)


    @staticmethod
    def run_params(dataframe):
        """ return run params based on dataframe
        """
        z=int(dataframe.iloc[0].z)
        x_min,y_min=dataframe[['x','y']].min().tolist()
        x_max,y_max=dataframe[['x','y']].max().tolist()
        sdate,edate=ClusterService.int_to_str_dates(
                dataframe.min_date.min(),
                dataframe.max_date.max())
        return {
            'z': z,
            'tile_bounds': [[x_min,y_min],[x_max,y_max]],
            'start_date': sdate,
            'end_date': edate }


    @staticmethod
    def int_to_str_dates(sdate,edate):
        """ convert date ints to date strs
        """
        sdate, edate=str(sdate), str(edate)
        sdate="{}-{}-{}".format(sdate[:4],sdate[4:6],sdate[6:])
        edate="{}-{}-{}".format(edate[:4],edate[4:6],edate[6:])
        return sdate, edate


    @staticmethod
    def lat(z,x,y,i=0,j=0):
        """ latitude from z/x/y/i/j
        """
        lat_rad=math.atan(math.sinh(math.pi*(1-(2*(y+(j/256.0))/(2**z)))))
        lat=(lat_rad*180.0)/math.pi
        return lat


    @staticmethod
    def lon(z,x,y,i=0,j=0):
        """ longitude from z/x/y/i/j
        """
        lon=(360.0/(2**z))*(x+(i/256.0))-180.0
        return lon


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
            bucket=DEFAULT_BUCKET,
            dataframe=None,
            errors_dataframe=None):
        self._init_properties()
        self.start_date=start_date
        self.end_date=end_date
        self.min_count=min_count
        self.width=width
        self.iterations=iterations
        self.z=z
        self.bucket=bucket
        self._dataframe=dataframe
        self._error_dataframe=errors_dataframe
        self._set_tile_bounds(bounds,tile_bounds,lon,lat,x,y)


    def run(self,max_processes=MAX_PROCESSES,force=False):
        """ find clusters on tiles

            Args:
                max_processes<int>: number of processes used in launching jobs
                force<bool[False]>: if true run even if dataframe is loaded
        """
        if (self._dataframe is not None) and (not force):
            print("WARNING: data already loaded pass 'force=True' to overwrite")
        else:
            try:
                # self.responses=None
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
                self._dataframe=None
                self._errors=None
            except Exception as e:
                print("ERROR: run failure -- {}".format(e))


    def name(self,ident=DEFAULT_CSV_IDENT):
        """ construct service name. use as default filename
        """
        return CSV_NAME_TMPL.format(
                ident,
                self.start_date,self.end_date,
                self.x_min,self.y_min,self.x_max,self.y_max,
                self.z,self.width,self.min_count,self.iterations)


    def urls(self,
            ident=DEFAULT_CSV_IDENT,
            region=DEFAULT_REGION,
            bucket=DEFAULT_BUCKET,
            url_base=None,
            errors=True):
        """ construct urls. using  default filename """
        return ClusterService.get_urls(
            self.name(ident),
            region,
            bucket,
            url_base,
            errors)


    def read(self,ident=DEFAULT_CSV_IDENT,
            local=False,
            region=DEFAULT_REGION,
            bucket=DEFAULT_BUCKET,
            url_base=None,
            errors=True):
        filename=self.name(ident)
        df,edf=ClusterService.get_dataframes(
            filename,
            local,
            region,
            bucket,
            url_base,
            errors)
        self._dataframe=df
        self._error_dataframe=edf


    def save(self,
            ident=DEFAULT_CSV_IDENT,
            filename=None,
            local=False,
            bucket=None,
            errors=True,
            temp_dir=None):
        """ write responses to csv

            Args:

                Use one of the following:
            
                    filename<str>: name/path of csv without '.csv' extension 
                    ident<str[DEFAULT_CSV_IDENT]>: prefix to default_name 
                
                Other arguments:
                
                    local<bool[False]>: if true write to local file else write to s3 file
                    bucket<str>: aws-bucket required if not local and not self.bucket
                    errors<bool[True]>: if true save errors-csv
        """
        if not filename: filename=self.name(ident)
        if temp_dir and local:
            filename = os.path.join(temp_dir, filename)
        if self._dataframe is None: self._process_responses()
        self._dataframe['alerts']=self._dataframe['alerts'].apply(lambda a: a.tolist())
        if local:
            self.dataframe(full=True).to_csv(
                "{}.csv".format(filename),
                index=None)
            if errors and self.errors().shape[0]:
                self.errors().to_csv(
                    "{}.errors.csv".format(filename),
                    index=None)
        else:
            obj=boto3.resource('s3').Object(
                bucket or self.bucket,
                "{}.csv".format(filename))
            obj.put(Body=self.dataframe(full=True).to_csv(None,index=None))
            obj.Acl().put(ACL=CSV_ACL)
            if errors and self.errors().shape[0]:
                obj=boto3.resource('s3').Object(
                    bucket or self.bucket,
                    "{}.errors.csv".format(filename))
                obj.put(Body=self.errors().to_csv(None,index=None))
                obj.Acl().put(ACL=CSV_ACL)
        self._dataframe['alerts']=self._dataframe['alerts'].apply(lambda a: np.array(a))

    def export(self,
               ident=DEFAULT_CSV_IDENT,
               format="PG",
               temp_dir=None,
               pg_table=None,
               pg_dbname=None,
               pg_host="localhost",
               pg_port=5432,
               pg_user=None,
               pg_password=None,
               concave=99,
               overwrite=False
               ):
        """ write responses to csv

            Args:

                Use one of the following:


                    ident<str[DEFAULT_CSV_IDENT]>: prefix to default_name
                    temp_dir<str>: Temporary folder for CSV export
                    pg_table<str>: PG table name
                    pg_conn<dict>: PostgreSQL connection with the following keys
                        dbname<string>: Database name.
                        port<integer>: Port.
                        user<string>: User name.
                        password<string>: Password.
                        host<string>: Server hostname.

                Other arguments:

                    overwrite<bool[False]>: if true write overwrite existing data

        """

        if format == "PG":

            if not pg_table:
                pg_table = self.name(ident).replace("%", "").replace(":", "").replace("-", "") + "_" + str(concave)

            # TODO add temp_dir to local_env
            filename = os.path.join(temp_dir, pg_table + ".csv")

            if self._dataframe is None:
                self._process_responses()

            self._dataframe['alerts'] = self._dataframe['alerts'].apply(
                lambda a: str(a.tolist()).replace('[', '{').replace(']', '}'))
            self.dataframe(full=True).to_csv(filename, index=None)

            # if errors and self.errors().shape[0]:
            #     self.errors().to_csv("{}.errors.csv".format(filename), index=None)

            if not pg_table:
                pg_table = os.path.basename(filename)

            conn = psycopg2.connect(database=pg_dbname, user=pg_user, password=pg_password, host=pg_host, port=pg_port)
            cur = conn.cursor()

            exists = False
            try:
                cur.execute("SELECT * FROM {} LIMIT 1;".format(pg_table))
                exists = True
            except:
                # make a commit here to avoid internal errors down the line
                conn.commit()

            if overwrite and exists:
                cur.execute("DELETE FROM {};".format(pg_table))

            elif not exists:
                sql = """
                    CREATE TABLE {0}
                    (
                      index integer NOT NULL,
                      count integer,
                      area integer,
                      min_date integer,
                      max_date integer,
                      longitude double precision,
                      latitude double precision,
                      z integer,
                      x integer,
                      y integer,
                      i integer,
                      j integer,
                      file_name text,
                      "timestamp" text,
                      alerts integer[],
                      CONSTRAINT {0}_pkey PRIMARY KEY (index)
                    )
                    WITH (
                      OIDS=FALSE
                    );
                    
                    SELECT AddGeometryColumn('{0}', 'multipoint', 4326, 'MULTIPOINT', 3);
                    SELECT AddGeometryColumn('{0}', 'concave', 4326, 'POLYGON', 2);

                    CREATE INDEX {0}_index_idx
                      ON {0}
                      USING btree
                      (index);

                    CREATE INDEX {0}_multipoint_idx
                      ON {0}
                      USING gist
                      (multipoint);

                    CREATE INDEX {0}_concave_idx
                      ON {0}
                      USING gist
                      (concave);
                    """.format(pg_table)

                cur.execute(sql)

            else:
                raise Exception('PG table already exist and overwrite set to false.')

            sql = """
                    CREATE OR REPLACE FUNCTION unnest_2d_1d(anyarray)
                      RETURNS SETOF anyarray AS
                    $BODY$
                    SELECT array_agg($1[d1][d2])
                    FROM   generate_subscripts($1,1) d1
                        ,  generate_subscripts($1,2) d2
                    GROUP  BY d1
                    ORDER  BY d1
                    $BODY$
                      LANGUAGE sql IMMUTABLE
                      COST 100
                      ROWS 1000;

                    CREATE OR REPLACE FUNCTION sinh(x numeric)
                      RETURNS double precision AS
                    $BODY$
                            BEGIN
                                    RETURN (exp(x) - exp(-x))/2;
                            END;
                    $BODY$
                      LANGUAGE plpgsql VOLATILE
                      COST 100;

                    COPY {0}(index,count,area,min_date,max_date,longitude,latitude,z,x,y,i,j,file_name,timestamp,alerts) 
                    FROM '{1}' DELIMITER ',' CSV HEADER;

                    WITH t AS (SELECT "index" AS id, z, x, y, unnest_2d_1d(alerts) AS alerts FROM {0})
                    UPDATE {0}
                    SET multipoint = g.multipoint
                    FROM
                    (SELECT 
                        id, 
                        st_collect(
                        ST_SetSRID(ST_MakePoint(
                            (360.0/(2^z))*(x+(alerts[2]/256.0))-180.0,
                            (atan(sinh((pi()*(1-(2*(y+(alerts[1]/256.0))/(2^z))))::numeric))*180.0)/pi(),
                            alerts[3]),
                        4326)) AS multipoint
                    FROM t
                    GROUP BY id) AS g
                    WHERE "index" = id;

                    UPDATE {0}
                    SET concave = ST_ConcaveHull (multipoint, {2});
                """.format(pg_table, filename, concave/100)

            # execute SQL and close connection
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()

            # delete temp file
            os.remove(filename)

            self._dataframe['alerts'] = self._dataframe['alerts'].apply(lambda a: np.array(a))

            # print("Data successfully exported to table {}".format(pg_table))

        else:
            raise Exception('Unsupported format.')

        return pg_table

    def request_size(self):
        """ get number of tiles in request
        """
        return (self.x_max-self.x_min+1)*(self.y_max-self.y_min+1)


    def bounds(self):
        """ get lat/lon-bounds
        """
        lat_min=ClusterService.lat(self.z,self.x_min,self.y_min,0,0)
        lat_max=ClusterService.lat(self.z,self.x_max,self.y_max,254.0,254.0)
        lon_min=ClusterService.lon(self.z,self.x_min,self.y_min,0,0)
        lon_max=ClusterService.lon(self.z,self.x_max,self.y_max,254.0,254.0)
        return [[lon_min,lat_min],[lon_max,lat_max]]


    def bounding_box(self):
        """ get lat/lon bounding box
        """
        mins,maxes=self.bounds()
        return [
            [mins[0],mins[1]],
            [maxes[0],mins[1]],
            [maxes[0],maxes[1]],
            [mins[0],maxes[1]],
            [mins[0],mins[1]]]


    def dataframe(self,full=False):
        """ return dataframe of clusters data

            Args:
                full<bool[False]>:
                    if true return full dataframe
                    otherwise only return VIEW_COLUMNS
        """
        if  self._dataframe is None:
            self._process_responses()
        if full:
            return self._dataframe
        else:
            return self._dataframe[VIEW_COLUMNS]


    def summary(self,dataframe=None):
        """ return nb_clusters,total-count/area,min_date,max_date

            Args:
                dataframe<dataframe>: if none use self.dataframe()
        """
        if dataframe is None: dataframe=self.dataframe()
        count=dataframe['count'].sum()
        area=dataframe.area.sum()
        # TODO does not return dates but NaN
        min_date,max_date=ClusterService.int_to_str_dates(
                dataframe.min_date.min(),
                dataframe.max_date.max())
        return dataframe.shape[0], count, area, min_date, max_date


    def tile(self,row_id=None,z=DEFAULT_ZOOM,x=None,y=None,full=False):
        """ return rows matching z,x,y

            Args:
                row_id<int>: row_id to get z/x/y from
                z,x,y<int,int,int>: if not row_id specify z/x/y
                full<bool[False]>:
                    if true return full dataframe
                    otherwise only return VIEW_COLUMNS
        """
        df=self.dataframe(full=True)
        if row_id is not None:
            row=df.iloc[row_id]
            z,x,y=row.z,row.x,row.y
        df=df[((df.z==z)&(df.x==x)&(df.y==y))]
        if full:
            return df
        else:
            return df[VIEW_COLUMNS]


    def errors(self):
        """ return error dataframe
        """
        if  self._dataframe is None:
            self._process_responses()
        return self._error_dataframe


    def cluster(self,
            row_id=None,
            lat=None,lon=None,
            z=None,x=None,y=None,i=None,j=None,
            timestamp=None,
            ascending=False,
            full=False):
        """ fetch data for single cluster

            Method for selecting row of dataframe

            Args:

                Use one of the following to select the row:

                    row_id<int>: dataframe index for cluster
                    lat,lon<floats>: latitude,longitude for cluster
                    z,x,y,i,j<ints>: tile/pixel location for cluster

                    (optional - really consider using row_id):
                        timestamp<str>: timestamp for cluster

                Other arguments:

                    ascending<bool>:
                        if true sort by ascending time and grab first matching row
                    full:
                        if false return only VIEW_COLUMNS.
                        else include all columns (including input/alerts data)
        """
        if self._not_none([row_id]):
            row=self.dataframe(full=True).iloc[row_id]
        else:
            test=True
            if self._not_none([lon,lat]):
                test=test & (
                    (self.dataframe(full=True).latitude==lat) &
                    (self.dataframe(full=True).longitude==lon))
            elif self._not_none([x,y,z]):
                test=test & (
                    (self.dataframe(full=True).z==z) &
                    (self.dataframe(full=True).x==x) &
                    (self.dataframe(full=True).y==y))
            if timestamp:
                test=test & (self.dataframe(full=True).timestamp==timestamp)
            rows=self.dataframe(full=True)[test]
            if ascending: rows.sort_values('timestamp',inplace=True)
            row=rows.iloc[0]
        if full:
            return row
        else:
            return row[VIEW_COLUMNS]


    def convex_hull(self,row_id=None,alerts=None):
        """ get convex_hull vertices for cluster

            Args:
                row_id<int>: if not alerts, dataframe row index for cluster
                alerts<array>: alerts for cluster
        """
        if alerts is None:
            alerts=self.dataframe(full=True).iloc[row_id].alerts
        return ConvexHull(alerts[:,0:2]).hull




    #
    #  INTERNAL METHODS
    #
    def _init_properties(self):
        self.x=None
        self.y=None


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


    def _set_tile_bounds(self,bounds,tile_bounds,lon,lat,x,y):
        """
            NOTE: if a single pair (x,y) or (lon,lat) the x,y-values 
            will be set for the find_by_tile method.
        """
        if bounds:
            tile_bounds=[self._lonlat_to_xy(*lonlat) for lonlat in bounds]
        elif (lat and lon):
            self.x,self.y=self._lonlat_to_xy(lon,lat)
            tile_bounds=[[self.x,self.y],[self.x,self.y]]
        elif (x and y):
            self.x=int(x)
            self.y=int(y)
            tile_bounds=[[self.x,self.y],[self.x,self.y]]
        tile_bounds=np.array(tile_bounds).astype(int)
        self.x_min,self.y_min=tile_bounds.min(axis=0)
        self.x_max,self.y_max=tile_bounds.max(axis=0)


    def _lonlat_to_xy(self,lon,lat):
        lat_rad=math.radians(lat)
        x=(2**self.z)*(lon+180.0)/360
        y=(2**self.z)*(1.0-math.log(math.tan(lat_rad)+(1/math.cos(lat_rad)))/math.pi)/2.0
        return int(x),int(y)


    def _process_response(self,x,y,response):
        if response:
            payload=json.loads(response.get('Payload',{}).read())
            processed_response=self._request_data(x,y,as_dict=True)
            if payload:
                processed_response.update(payload)
            return processed_response
        return None


    def _run_tile(self,location=None,x=None,y=None):
        """ find clusters on tile
        
            NOTE: if no args are passed it will attempt to use 
                  the x,y (or lon,lat) passed in the constructor
        
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
                error_data['data']={ 'x':x, 'y': y }
                error_data['error']="{}".format(e)
                error_data['error_trace']="service.1"
                return error_data


    def _process_responses(self):
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
                    ClusterService.lon(z,x,y,i,j),
                    ClusterService.lat(z,x,y,i,j),
                    z,x,y,i,j,
                    response['file_name'],
                    response['timestamp'],
                    np.array(cluster.get('alerts')).astype(int)])
        return rrows


    def _error_row(self,error,response):
        error_trace=response.get('error_trace','service.2')
        z=response.get('z') or self.z
        x=response.get('x')
        y=response.get('y')
        if (z and x and y):
            lon=ClusterService.lon(int(z),int(x),int(y),128,128)
            lat=ClusterService.lat(int(z),int(x),int(y),128,128)
        else:
            lon,lat=None,None
        return [z,x,y,lon,lat,error,error_trace]


    def _not_none(self,values):
        test=[ (val is not None) for val in values ]
        return np.prod(test).astype(bool)


class ToListAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(ToListAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values:
            setattr(namespace, self.dest, json.loads(values))


class ToLatLon(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, "lon", values[0])
        setattr(namespace, "lat", values[1])


class ToXY(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, "x", values[0])
        setattr(namespace, "y", values[1])

#
# Main
#


def main():
    ## Service parsers

    service_parser = ArgumentParser(add_help=False)

    # coordinate group (mutual)

    coord_group_m = service_parser.add_mutually_exclusive_group(required=True)
    coord_group_m.add_argument("--lonlat", nargs=2, type=float, action=ToLatLon,
                               metavar=("LON", "LAT"),
                               help="Longitude and latitude, use to run a single tile")
    coord_group_m.add_argument("--bounds", type=str, action=ToListAction,
                               metavar=[["minLON", "minLAT"], ["maxLON", "maxLAT"]],
                               help="Bounding box in lat/lon")
    coord_group_m.add_argument("--xy", nargs=2, type=int, #action=ToXY,
                               metavar=("X", "Y"),
                               help="X/Y tile index (z is always set to 12), use  to run a single tile")
    coord_group_m.add_argument("--tile_bounds", type=str, action=ToListAction,
                               metavar=[["minX", "minY"], ["maxX", "maxY"]],
                               help="Bounding box for x/y tiles")

    # Cluster group
    cluster_group = service_parser.add_argument_group("Cluster settings", "Configure the cluster.")

    cluster_group.add_argument("-w", "--width", dest="width",
                               help="Gaussian width in cluster algorithm", type=int, default=5)

    cluster_group.add_argument("-c", "--min_count", dest="min_count",
                               help="Minimum number of alerts in a cluster", type=int, default=25)
    cluster_group.add_argument("-i", "--iterations", dest="iterations",
                               help="Number of times to iterate when finding clusters", type=int, default=25)

    # Date group
    date_group = service_parser.add_argument_group("Dates", "Set start and end date.")

    date_group.add_argument("--start_date", dest="start_date", type=str,
                            metavar="YYYY-MM-DD", help="Start date")
    date_group.add_argument("--end_date", dest="end_date", type=str,
                            metavar="YYYY-MM-DD", help="End date (optional), default today")

    ## Save parser
    save_parser = ArgumentParser(add_help=False)

    # Save group
    save_group = save_parser.add_argument_group("Save settings","Save data.")

    save_group.add_argument("-f", "--file", dest="filename", type=str,
                            help="CSV filename")
    save_group.add_argument("--local", dest="local", action="store_true",
                            help="If set, save file locally")
    save_group.add_argument("--bucket", dest="bucket", type=str,
                            help="S3 bucket in which CSV file will be saved (optional)")
    save_group.add_argument("--temp_dir", dest="temp_dir", type=str,
                            help="Temp directory")

    ## Export parser
    export_parser = ArgumentParser(add_help=False)

    # Export group
    export_group = export_parser.add_argument_group("Export settings", "Export data.")
    export_group.add_argument("--format", dest="format", choices=["PG"],
                               help="Export format (default PG)", default="PG")
    export_group.add_argument("--pg_table", dest="pg_table", type=str,
                               help="PostgreSQL table name")
    export_group.add_argument("--pg_dbname", dest="pg_dbname", type=str,
                               required=True, help="PostgreSQL database name")
    export_group.add_argument("--pg_host", dest="pg_host", type=str,
                               help="PostgreSQL host")
    export_group.add_argument("--pg_port", dest="pg_port", type=str,
                               help="PostgreSQL port")
    export_group.add_argument("--pg_user", dest="pg_user", type=str,
                              required=True, help="PostgreSQL user")
    export_group.add_argument("--pg_password", dest="pg_password", type=str,
                              required=True, help="PostgreSQL password")
    export_group.add_argument("--concave", dest="concave", type=int,
                              help="Target percent of area for concave hull. Integers between 0 and 100."\
                                    "When set to 100, area is equal to convex hull")
    export_group.add_argument("--temp_dir", dest="temp_dir", type=str,
                              required=True, help="Temp directory")
    export_group.add_argument("--overwrite", dest="overwrite", action='store_true',
                              help="Overwrite existing table")

    # Main parser
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    # Subparser INFO
    parser_info = subparsers.add_parser('info', parents=[service_parser], help='Print cluster service info')
    parser_info.set_defaults(func=_print_info)

    # Subparser RUN
    parser_run = subparsers.add_parser('run', parents=[service_parser, save_parser], help='Run cluster service and save to CSV')
    parser_run.set_defaults(func=_run)

    # Subparser EXPORT
    parser_export = subparsers.add_parser('export', parents=[service_parser, export_parser], help='Run cluster service and export results to selected format')
    parser_export.set_defaults(func=_export)

    args = parser.parse_args()
    args.func(args)


def _run(args):
    service = _run_service(args)
    _save_service(service, args)


def _run_service(args):
    service=_print_info(args,True)
    print("\nRUN: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    service.run()
    nb_clusters,count,area,min_date,max_date=service.summary()
    print("\tNB CLUSTERS: {}".format(nb_clusters))
    print("\tNB ERRORS: {}".format(service.errors().shape[0]))
    print("\tTOTAL COUNT: {}".format(count))
    print("\tTOTAL AREA: {}".format(area))
    print("\tDATES: {} to {}".format(min_date,max_date))
    return service


def _save_service(service, args):

    kwargs = copy.deepcopy(vars(args))
    arg_spec = inspect.getargspec(ClusterService.save)

    service = _run_service(args)

    for key in kwargs.keys():
        if key not in arg_spec[0]:
            kwargs.pop(key)
        elif not kwargs[key] or (isinstance(kwargs[key], dict) and len(arg_spec[key] == 0)):
            kwargs.pop(key)

    print("SAVE: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    service.save(**kwargs)
    print("\tfilename: {}".format(service.name()))
    print("COMPLETE: {}\n\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def _export(args):

    kwargs = copy.deepcopy(vars(args))
    arg_spec = inspect.getargspec(ClusterService.export)

    service = _run_service(args)

    for key in kwargs.keys():
        if key not in arg_spec[0]:
            kwargs.pop(key)
        elif not kwargs[key] or (isinstance(kwargs[key], dict) and len(arg_spec[key] == 0)):
            kwargs.pop(key)

    print("EXPORT: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    pg_table = service.export(**kwargs)
    print("\tpg_table: {}".format(pg_table))
    print("COMPLETE: {}\n\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def _print_info(args,return_service=False):

    arg_spec = inspect.getargspec(ClusterService.__init__)

    kwargs = copy.deepcopy(vars(args))

    for key in kwargs.keys():
        if key not in arg_spec[0]:
            kwargs.pop(key)
        elif not kwargs[key] or (isinstance(kwargs[key], dict) and len(kwargs[key] == 0)):
            kwargs.pop(key)

    service = ClusterService(**kwargs)

    print("\n\nClusterService:")
    print("\trequest_size:",service.request_size())
    print("\tbounds:",service.bounds())
    print("\tdate-range: {} to {}".format(service.start_date,service.end_date))
    print("\twidth:",service.width)
    print("\tmin_count:",service.min_count)
    print("\titerations:",service.iterations)
    if return_service:
        return service
    else:
        print("\n")


if __name__ == "__main__":
    main()



