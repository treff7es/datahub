# Airflow ingestion DAG

This is an Airflow DAG which captures task/dag properties and the taks lineage at places where lineage backend can't be used.

## Installation

1. Add `acryl-datahub[airflow]` to your Airflow's requirements file.
On Amazon Managed Workflows for Apache Airflow you can find the [here](https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html) how to do it.

2. Setup an Airflow connection to your datahub gms instance 
* **Airflow 2**: 
Setup a `datahub_rest` or a `datahub_kafka` connection which will be provided by the 
datahub airflow package.

Example for Datahub REST Server connection:
```
Conn Id: datahub
Conn Type: DataHub REST Server
Host: https://mygmsinstance/gms
Password: yourtoken
```

* **Airflow 1**:
On Airflow 1 you might not have this and it is fine if you setup a http connection.

Example for HTTP connection:
```
Conn Id: datahub
Conn Type: HTTP
Host: https://mygmsinstance/gms
Password: yourtoken
```

3. Copy the dag python code and the config.yaml under your dag folder
4. Edit config yaml file to match your need.
5. Change the schedule_interval in the dag python to your need. By default it runs daily. 

## Usage
If everything is set up properly then after a successful DAG (it can be even manual or scheduled) run the captured metada and task lineage should show up on Datahub UI under the name which is set in the config's `cluster` property. 


## Configuration parameters

| Field                    | Required | Default | Description                                                              |
|--------------------------|----------|---------|--------------------------------------------------------------------------|
| `datahub_conn_id`        | ✅        | datahub | The id of the airflow connection which points to datahub gms             |
| `cluster`                | ✅        | PROD    | This name identifies your Airflow cluster on Datahub.                    |
| `capture_ownership_info` |          | true    | Should it capture ownership info based on the Airflow dag/task ownership |
| `capture_tags_info`      |          | true    | Should it capture tags info based on the Airflow dag/task tags           |

