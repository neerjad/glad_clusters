# MEAN SHIFT CLUSTERING FOR AWS LAMBDA

Initial Hacks at clustering with AWS Lambda

---
##### SERVERLESS

We use [serverless](https://serverless.com/)
to deploy our AWS lambda functions and [serverless-python-requirements](https://github.com/UnitedIncome/serverless-python-requirements) to manage our python modules. Modules listed in `requirements.txt`, which in turn is generated using a virtualenv (mslambda), are installed on our lambda instances. 

##### ENVIRONMENTS

Its important to keep our requirements to a minimum due to restrictions on the size of our lambda instances.  However, deployment can take awhile (several minutes) so we want to be able to test and run the code locally. As such, we use our global environment for local testing (which has boto3 installed) and the virtualenv for the serverless deployment. 

---
RUN LOCAL TESTS:

```bash
source deactivate
python handler.py '{"file":"gc7"}'
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
sls invoke -f meanshift -l -d '{"file":"gc7"}'
```


---
##### NOTES

---
###### TABLE DATA:

- CLUSTER DATA
    + location (tile: i,j)
    + number of alerts
    + area of convexhull
    + max/min date of alerts
    + original alert locations
- RUN SPECS
    + max/min date
    + threshold
    + width
    + min_count
    + x/y/z
    + optional
        * hard threshold
        * intensity threshold
        * iterations
- OTHER OPTIONAL
    + file_name
    + input_data




