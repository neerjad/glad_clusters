# GLAD CLUSTERS
_Mean Shift Clustering with AWS Lambda_

This package uses AWS-Lambda to perform meanshift clustering on GLAD tiles.
User interaction happens through two main classes:  `ClusterService`, used to run the clustering algorithm/read the clustered data, and `ClusterViewer`  used display the data received in a python notebook. Additionally there is a [CLI](#cli) that allows you to run the cluster algorithm from the command-line. 

Python-Doc-Strings have been used throughout.  See the code base or use `help(ClusterService/Viewer)` for documentation.  An complete example is given in this [notebook](https://github.com/wri/mean_shift_lambda/blob/master/nb_archive/ClusterServiceViewer.ipynb), 

1. [Install](#install)
2. [Super Quick Quick Start](#quick)
3. [Command Line Interface](#cli)
4. [Dev](#dev)

a super quick quick-start:

<a name='install'></a>
---
## INSTALL
```
# clone repo
git clone https://github.com/wri/glad_clusters.git

# cd into repo and install with pip
pip install -e .
```


<a name='quick'></a>
---
## QUICK START

```python
from utils.service import ClusterService
from utils.viewer import ClusterViewer
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
---
## COMMAND-LINE-INTERFACE

When you use pip to install the cluster service a CLI is installed along with it. There are two optional "run_types": `info` which just returns data about a potential run, and `run` which runs the service and saves the data.

#### EXAMPLE
```bash
$ glad_clusters info '{"x":1365,"y":2082}'


ClusterService:
    request_size: 1
    bounds: [[-60.029296875, -2.98692739333487], [-59.94175091911765, -3.0743508993624977]]
    date-range: 2015-01-01 to 2018-02-09
    width: 5
    min_count: 25
    iterations: 25


$ glad_clusters run '{"x":1365,"y":2082}'


ClusterService:
    request_size: 1
    bounds: [[-60.029296875, -2.98692739333487], [-59.94175091911765, -3.0743508993624977]]
    date-range: 2015-01-01 to 2018-02-09
    width: 5
    min_count: 25
    iterations: 25

RUN: 2018-02-09 13:12:59
    NB CLUSTERS: 20
    TOTAL COUNT: 1455
    TOTAL AREA: 7996
    DATES: 2015-03-28 to 2018-01-30
SAVE: 2018-02-09 13:13:14
    filename: clusters_2015-01-01:2018-02-09_1365:2082:1365:2082_12:5:25:25
COMPLETE: 2018-02-09 13:13:15

```


#### HELP DOCS
```bash
$ glad_clusters --help
usage: glad_clusters [-h] run_type data

GLAD Cluster Service: Meanshift clustering for GLAD alerts.

positional arguments:
  run_type    run-type: one of run, info
  data        json string for any of the keyword arguments in ClusterService()

optional arguments:
  -h, --help  show this help message and exit
```


<a name='dev'></a>
---
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




