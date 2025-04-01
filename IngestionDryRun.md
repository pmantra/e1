# File Ingestion Dry Run

This document describes the steps to dry run ingesting a census file

## Step 1. Prepare dry run file
All dry run files should be put into `dryrun` folder under the GCS bucket.

In production, it is [here](https://console.cloud.google.com/storage/browser/5d75e28d-6c95-2162-1731-719558f94634/dryrun;tab=objects?inv=1&invt=AbkBvQ&organizationId=171898763053&project=maven-clinic-ftp-1&prefix=&forceOnObjectsSortingFiltering=false)

The dry run code takes in file using a file name convention as following:
```python
{organization_id}_xxxxx.csv
```
For example: `71_20241205_COZEN.csv`, 71 is the organization_id.

**Copy the file to `dryrun` folder and rename it according naming convention**

## Step 2. Run dry run code in tsh
- Run the following shell command to get into e9y-worker pod
    ```bash
    gcloud auth login --update-adc
    # in prod env
    tsh kube login maven-clinic-prod_us-central1_prod
    kubectl exec -it deployment/eligibility-worker -c worker -- bash
    ```
- On the pod, start python env using:
  ```
  /.venv/bin/python -m asyncio
  ```
- Run dry run code
  ```python
  from app.dryrun import dryrun
  # replace the file name with the one in step 1
  file_name = '71_20241205_COZEN.csv' 
  await dryrun.process_dryrun(file_name)
  ```
  The dry run code will write report to `dryrun/{file_name}` (e.g. `dryrun/71_20241205_COZEN/`) folder, which contains the following:

  - `summary.txt` contains the dry run summary
  - `file.csv` is the file table record for the dry run in csv format 
  - `errors.csv` contains file parse errors 
  - `{organization_id}_non_pop_member.csv` contains members without subpop_id, organization_id may differ from input file, in case sub orgs exists.


### Override sub population

In the dry run, when calculate sub population, calcuator will try:
- get active population first
- if no active population found, use the most recent created population.

You can override above logic by passing in `override_sub_population`.
- `override_sub_population` is dictionary mapping `organization_id` -> `population_id`
e.g:
```python
from app.dryrun import dryrun
override_sub_population = {
  2018: 1049
}
file_name = '2018_20241212_OhioHealth_OpenEnrollment.csv'
await dryrun.process_dryrun(file_name, override_sub_population=override_sub_population)
```