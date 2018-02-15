# GLAD CLUSTERS
_Mean Shift Clustering with AWS Lambda_

This package uses AWS-Lambda to perform meanshift clustering on GLAD tiles.
User interaction happens through two main classes:  `ClusterService`, used to run the clustering algorithm/read the clustered data, and `ClusterViewer`  used display the data received in a python notebook. Additionally there is a [CLI](#cli) that allows you to run the cluster algorithm from the command-line. 

Python-Doc-Strings have been used throughout.  See the code base or use `help(ClusterService/Viewer)` for documentation.  An complete example is given in this [notebook](https://github.com/wri/mean_shift_lambda/blob/master/nb_archive/ClusterServiceViewer.ipynb), 

1. [Install](#install)
1. [Pixel Coordinates and Image Data](#coords)
2. [Super Quick Quick Start](#quick)
3. [Command Line Interface](#cli)
4. [Dev](#dev)

<a name='install'></a>
## INSTALL

```
# clone repo
git clone https://github.com/wri/glad_clusters.git

# cd into repo and install with pip
pip install -e .
```

<a name='coords'></a>
## A SHORT NOTE ON COORDINATES

_TLDR; Alert data values are \[j, i, days\-since\-20150101\], where j is the pixel coordinate along the y axis and i is the pixel coordinate along the x axis._

As is standard, we use `z/x/y` to represent our tile coordinates. `i` and `j`  are used for the pixel coordinates within the tile, which run along the `x` and `y` axis respectively. Note however we are using skimage whose reads in numpy arrays where the first position is along the `y` axis and the second is along the `x` axis.  It is important to keep in mind then that the alerts column in `cluster_service.dataframe(full=True)`  gives a list of alert data whose values are [j, i, days-since-20150101], rather than the more intuitive [i,j,...].


<a name='quick'></a>
## QUICK START

```python
from glad_clusters.utils.service import ClusterService
from glad_clusters.utils.viewer import ClusterViewer
%matplotlib inline


# define lon/lat bounding box:
bounds=[[12.1823368669, -13.2572266578], [15.1741492042, -10.25608775474]]

# get data
c=ClusterService(bounds=bounds)
c.run()

# save data (grab filename for later use)
filename=c.name()
c.save()

# load previously saved data from params
c=ClusterService(bounds=bounds)
c.read()

# OR: load previously saved data from filename
c=ClusterService.read_csv(filename)

# initialize viewer
view=ClusterViewer(c)
    
# view input data for row 182
view.tile(182)

# view cluster for row 182
view.cluster(182)

# view clusters 182,4,185,6 in a single row 
view.clusters(row_ids=[182,4,185,6])

# view clusters 182 through 184 in a single row
view.clusters(start=182,end=185)
```


<a name='cli'></a>
## COMMAND-LINE-INTERFACE

When you use pip to install the cluster service a CLI is installed along with it. There are two optional "run_types": `info` which just returns data about a potential run, and `run` which runs the service and saves the data.

#### EXAMPLE
```bash
$ glad_clusters info -x 1365 -y 2082


ClusterService:
    request_size: 1
    bounds: [[-60.029296875, -2.98692739333487], [-59.94175091911765, -3.0743508993624977]]
    date-range: 2015-01-01 to 2018-02-09
    width: 5
    min_count: 25
    iterations: 25


$ glad_clusters run -x 1365 -y 2082


ClusterService:
    request_size: 1
    bounds: [[-60.029296875, -2.98692739333487], [-59.94175091911765, -3.0743508993624977]]
    date-range: 2015-01-01 to 2018-02-09
    width: 5
    min_count: 25
    iterations: 25

RUN: 2018-02-09 13:12:59
    NB CLUSTERS: 20
    NB ERRORS: 0
    TOTAL COUNT: 1455
    TOTAL AREA: 7996
    DATES: 2015-03-28 to 2018-01-30
SAVE: 2018-02-09 13:13:14
    filename: clusters_2015-01-01%2018-02-09_1365%2082%1365%2082_12%5%25%25
COMPLETE: 2018-02-09 13:13:15


$ glad_clusters export -x 1365 -y 2082 --pg_dbname scratch --pg_user postgres --pg_password secret


ClusterService:
    request_size: 1
    bounds: [[-60.029296875, -2.98692739333487], [-59.94175091911765, -3.0743508993624977]]
    date-range: 2015-01-01 to 2018-02-09
    width: 5
    min_count: 25
    iterations: 25

RUN: 2018-02-09 13:12:59
    NB CLUSTERS: 20
    NB ERRORS: 0
    TOTAL COUNT: 1455
    TOTAL AREA: 7996
    DATES: 2015-03-28 to 2018-01-30
EXPORT: 2018-02-09 13:13:14
    pg_table: clusters_2015010120180209_1365208213652082_1252525_99
COMPLETE: 2018-02-09 13:13:15

```


#### HELP DOCS
```bash
$ glad_clusters --help
usage: glad_clusters {info,run,export} ...

positional arguments:
  {info,run,export}  sub-command help
    info             Print cluster service info
    run              Run cluster service and save to CSV
    export           Run cluster service and export results to selected format

optional arguments:
  -h, --help         show this help message and exit
```
  
```bash
usage: glad_cluster info [-h]
                       (--lonlat LON LAT | --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']] | --xy X Y | --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']])
                       [-w WIDTH] [-c MIN_COUNT] [-i ITERATIONS]
                       [--start_date YYYY-MM-DD] [--end_date YYYY-MM-DD]

optional arguments:
  -h, --help            show this help message and exit
  --lonlat LON LAT      Longitude and latitude, use to run a single tile
  --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']]
                        Bounding box in lat/lon
  --xy X Y              X/Y tile index (z is always set to 12), use to run a
                        single tile
  --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']]
                        Bounding box for x/y tiles

Cluster settings:
  Configure the cluster.

  -w WIDTH, --width WIDTH
                        Gaussian width in cluster algorithm
  -c MIN_COUNT, --min_count MIN_COUNT
                        Minimum number of alerts in a cluster
  -i ITERATIONS, --iterations ITERATIONS
                        Number of times to iterate when finding clusters

Dates:
  Set start and end date.

  --start_date YYYY-MM-DD
                        Start date
  --end_date YYYY-MM-DD
                        End date (optional), default today

```

```bash
usage: glad_clusters run [-h]
                      (--lonlat LON LAT | --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']] | --xy X Y | --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']])
                      [-w WIDTH] [-c MIN_COUNT] [-i ITERATIONS]
                      [--start_date YYYY-MM-DD] [--end_date YYYY-MM-DD]
                      [-f FILENAME] [--local] [--bucket BUCKET]
                      [--temp_dir TEMP_DIR]

optional arguments:
  -h, --help            show this help message and exit
  --lonlat LON LAT      Longitude and latitude, use to run a single tile
  --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']]
                        Bounding box in lat/lon
  --xy X Y              X/Y tile index (z is always set to 12), use to run a
                        single tile
  --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']]
                        Bounding box for x/y tiles

Cluster settings:
  Configure the cluster.

  -w WIDTH, --width WIDTH
                        Gaussian width in cluster algorithm
  -c MIN_COUNT, --min_count MIN_COUNT
                        Minimum number of alerts in a cluster
  -i ITERATIONS, --iterations ITERATIONS
                        Number of times to iterate when finding clusters

Dates:
  Set start and end date.

  --start_date YYYY-MM-DD
                        Start date
  --end_date YYYY-MM-DD
                        End date (optional), default today

Save settings:
  Save data.

  -f FILENAME, --file FILENAME
                        CSV filename
  --local               If set, save file locally
  --bucket BUCKET       S3 bucket in which CSV file will be saved (optional)
  --temp_dir TEMP_DIR   Temp directory
```

```bash
usage: glad_clusters export [-h]
                         (--lonlat LON LAT | --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']] | --xy X Y | --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']])
                         [-w WIDTH] [-c MIN_COUNT] [-i ITERATIONS]
                         [--start_date YYYY-MM-DD] [--end_date YYYY-MM-DD]
                         [--format {PG}] [--pg_table PG_TABLE] --pg_dbname
                         PG_DBNAME [--pg_host PG_HOST] [--pg_port PG_PORT]
                         --pg_user PG_USER --pg_password PG_PASSWORD
                         [--concave CONCAVE] --temp_dir TEMP_DIR [--overwrite]

optional arguments:
  -h, --help            show this help message and exit
  --lonlat LON LAT      Longitude and latitude, use to run a single tile
  --bounds [['minLON', 'minLAT'], ['maxLON', 'maxLAT']]
                        Bounding box in lat/lon
  --xy X Y              X/Y tile index (z is always set to 12), use to run a
                        single tile
  --tile_bounds [['minX', 'minY'], ['maxX', 'maxY']]
                        Bounding box for x/y tiles

Cluster settings:
  Configure the cluster.

  -w WIDTH, --width WIDTH
                        Gaussian width in cluster algorithm
  -c MIN_COUNT, --min_count MIN_COUNT
                        Minimum number of alerts in a cluster
  -i ITERATIONS, --iterations ITERATIONS
                        Number of times to iterate when finding clusters

Dates:
  Set start and end date.

  --start_date YYYY-MM-DD
                        Start date
  --end_date YYYY-MM-DD
                        End date (optional), default today

Export settings:
  Export data.

  --format {PG}         Export format (default PG)
  --pg_table PG_TABLE   PostgreSQL table name
  --pg_dbname PG_DBNAME
                        PostgreSQL database name
  --pg_host PG_HOST     PostgreSQL host
  --pg_port PG_PORT     PostgreSQL port
  --pg_user PG_USER     PostgreSQL user
  --pg_password PG_PASSWORD
                        PostgreSQL password
  --concave CONCAVE     Target percent of area for concave hull. Integers
                        between 0 and 100.When set to 100, area is equal to
                        convex hull
  --temp_dir TEMP_DIR   Temp directory
  --overwrite           Overwrite existing table
```


<a name='dev'></a>
## DEVELOPMENT

##### SERVERLESS

We use [serverless](https://serverless.com/)
to deploy our AWS lambda functions and [serverless-python-requirements](https://github.com/UnitedIncome/serverless-python-requirements) to manage our python modules. Modules listed in `requirements.txt`, which in turn is generated using a virtualenv (mslambda), are installed on our lambda instances. 

##### ENVIRONMENTS

Its important to keep our requirements to a minimum due to restrictions on the size of our lambda instances.  However, deployment can take awhile (several minutes) so we want to be able to test and run the code locally. As such, we use our global environment for local testing (which has boto3 installed) and the virtualenv for the serverless deployment. 

---
RUN LOCAL TESTS:

```bash
source deactivate
python local_env.py dev

python handler.py '{"z":12,"x":1355,"y":2045,"start_date":"2016-06-01","end_date":"2016-12-01"}'
```

---
DEPLOY AND RUN TEST ON LAMBDA INSTANCE:
```bash
source activate mslambda
# deploy (verbose)
sls deploy -v
# or deploy only the function
sls deploy function -f meanshift

# invoke test (with logs)
sls invoke -f meanshift -l -d '{"z":12,"x":1355,"y":2045,"start_date":"2016-06-01","end_date":"2016-12-01"}'
```


---




