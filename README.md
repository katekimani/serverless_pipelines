# OVERVIEW & PURPOSE

The technology we use today has become integral to our lives and increasingly we expect it to be always available and responsive to our unique needs, whether its showing us when a service is almost failing or auto playing a relevant song based on our play history. As software engineers, we are responsible for delivering this technology to meet these expectations and increasingly we rely on data (massive) to make that possible.

In this workshop, we explore the process of organizing the data we receive to allow for real time analytics. We use the example use case of an organization needing to visualize the logs from the backend servers to a dashboard. 

# SCOPE

- Transform raw data to processed data.
- GCP.

# OBJECTIVES

- Explain what a data pipeline is.
- Give an overview of the evolution of data pipelines.
- Build a working example using GCP Dataflow and BigQuery

# PREREQUISITES

Basic Python knowledge

# OUTCOMES

_Success looks like:_
- Data read from a plain text/csv file loaded to an analytics DB.
- Attendees able to run the code on their own.

### CONTENT

# DATA PIPELINE
## DEFINITION: 

A set of data processing elements connected in series where the output of one element is the input of the next one. 
_source [wikipedia](https://en.wikipedia.org/wiki/Pipeline_(computing))_

## PROPERTIES:

- Low event latency
    - Able to query recent events data within seconds of availability.
- Scalability
    - Adapt to growing data as product use rises & ensure data is available for querying.
- Interactive querying
    - Support both long-running batch queries and smaller interactive queries without delays.
- Versioning
    - Ability to make changes to the pipeline and data definitions without downtime or data loss.
- Monitoring
    - Generate alerts if data expected is not being received.
- Testing
    - Able to test pipeline components while ensuring test events are not added to storage.

## TYPES OF DATA

- Raw data
    - Unprocessed data in format used on source e.g JSON
    - No schema applied
- Processed data
    - Raw data with schema applied
    - Stored in event tables/destinations in pipelines
- Cooked data
    - Processed data that has been summarized.
    
## MOTIVATION FOR DATA PIPELINE

Why are you collecting and herding the data you have?
Ask the end user what they want to ask or answer about the app or data. E.g how much traffic are we supporting, what is the peak period, location e.t.c.

## PIPELINES EVOLUTION

- Flat file era
    - Save logs on server.
- Database era
    - Data staged in txt or csv is loaded to database.
- Datalake era
    - Data staged in hadoop/S3 is loaded to database.
- Serverless era
    - Managed services are used for storage and querying.

## GOTCHAS

- Central point of failure
- Bottlenecks due to too many events from one source.
    - Query performance degradation where a query takes hours to complete.

## KEY CONCEPTS IN APACHE BEAM

- Pipeline
    - Encapsulates the workflow of the entire data processing tasks from start to finish.
- PCollection
    - Represents a distributed dataset that the beam pipeline operates on.
- PTransform
    - Represents a data processing operation or a step in the pipeline.
    - ParDo
        - Beam transformation for generic parallel processing.
    - DoFn
       - Applies the logic to each element in the input PCollection and populates the elements of an output collection.

## CODE
- Setup python environment and install beam https://beam.apache.org/get-started/quickstart-py/
 *Requires python2.7*
- Start virtual environment  e.g `source venv/bin/activate`

## GOOGLE CLOUD PLATFORM 
- Dataflow setup - https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python
- Sign up for free trial (Card required, $300 free )
- Create a project
- Create a storage bucket
- Ensure dataflow api is enabled.

## CODE EXECUTION 
- Run the code remotely on GCP example (from the root folder of the code): 
   ```
   python -m kiva_org.loans \
      --runner=DataflowRunner \
      --project=kiva-org-pipeline \
      --temp_location=gs://kiva_org_pipeline/tmp \
      --staging_location=gs://kiva_org_pipeline/staging \
      --output=gs://kiva_org_pipeline/results/loans \
      --input=gs://kiva_org_pipeline/data/kiva/loans.csv
   ```
 - Run the code locally example:
    `python -m kiva_org.loans --runner=DirectRunner`
 - Run the big query version of the code remotely example:
    ```
    python -m kiva_org.loans_bigquery \
       --runner=DataflowRunner \
       --project=kiva-org-pipeline \
       --temp_location=gs://kiva_org_pipeline/tmp \
       --staging_location=gs://kiva_org_pipeline/staging \
       --output=gs://kiva_org_pipeline/results/loans \
       --input=gs://kiva_org_pipeline/data/kiva/loans.csv  \
       --table_name=kiva_loans_summary \
       --dataset=kiva_dataset
     ```

## REFERENCES

- https://towardsdatascience.com/data-science-for-startups-data-pipelines-786f6746a59a
- https://beam.apache.org/get-started/mobile-gaming-example/
- https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/
- https://www.coursera.org/learn/serverless-data-analysis-bigquery-cloud-dataflow-gcp/

