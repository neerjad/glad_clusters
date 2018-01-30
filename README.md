# MEAN SHIFT CLUSTERING FOR AWS LAMBDA

_Clustering with AWS Lambda_

User interaction happens through two main classes:  `ClusterService`, used to run the clustering algorithm, and `ClusterViewer`  used display the data received in a python notebook. _TODO: There is also a command-line-interface for running the clustering algorithm and saving the responses to csv files_


---
## ClusterService

```
from utils.service import ClusterService

# initialize by tile-x,y bounds
c=ClusterService(tile_bounds=[[1355,2045],[1356,2046]])

# check the number of tiles you are about to run
c.request_size()

# run by tile x,y values
%time c.run()

# or fetch the data from a previous run
%time c.fetch()

# the results as dataframe
c.view().head()

# sort results by 'area' in descending order 
c.view().sort_values('area',ascending=False).head()

# the full-results as dataframe (including input data and initial points)
c.dataframe().head()

# errors that may have occurred 
c.errors().head()

# a particular row
c.cluster(row_id=row_id)

# a particular full-row
c.cluster(row_id=row_id,as_view=False)
```


---
## ClusterViewer

```
from utils.service import ClusterService
from utils.viewer import ClusterViewer
%matplotlib inline

# get data
c=ClusterService(tile_bounds=[[1355,2045],[1356,2046]])
%time c.fetch()
c.view().head()

# initialize viewer
view=ClusterViewer(c)
    
# view input data for row 182
view.tile(182)

# view cluster for row 182
view.cluster(182)

# view clusters 182,4,185,6 in a single row 
view.clusters(row_ids=[82,3,184,6])

# view clusters 182 through 184 in a single row
view.clusters(start=182,end=185)
```

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
##### NOTES

_nothing to see here_




