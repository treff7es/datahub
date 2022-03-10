import os
from datetime import datetime
from enum import Enum
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from airflow import DAG, settings
from airflow.configuration import conf
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.serialized_objects import SerializedDAG
from datahub.configuration.common import ConfigModel
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub_provider.hooks.datahub import AIRFLOW_1, DatahubGenericHook
from sqlalchemy.orm import Session

if AIRFLOW_1:
    from airflow.operators.python_operator import PythonOperator
    from airflow.hooks.base_hook import BaseHook
    from airflow.utils.db import create_session
else:
    from airflow.operators.python import PythonOperator
    from airflow.hooks.base import BaseHook
    from airflow.utils.session import create_session

class AirflowIngestConfig(ConfigModel):
    datahub_conn_id: str = "datahub"
    cluster: str = "PROD"
    capture_ownership_info: bool = True
    capture_tags_info: bool = True
    graceful_exceptions: bool = True


# This was copied here from Airflow 2 due to compatibility issues with Airflow 1
class AirflowState(str, Enum):
    """
    Enum that represents all possible states that a Task Instance can be in.

    Note that None is also allowed, so always use this in a type hint with Optional.
    """

    # Set by the scheduler
    # None - Task is created but should not run yet
    REMOVED = "removed"  # Task vanished from DAG before it ran
    SCHEDULED = "scheduled"  # Task should run and will be handed to executor soon

    # Set by the task instance itself
    QUEUED = "queued"  # Executor has enqueued the task
    RUNNING = "running"  # Task is executing
    SUCCESS = "success"  # Task completed
    SHUTDOWN = (
        "shutdown"  # External request to shut down (e.g. marked failed when running)
    )
    RESTARTING = "restarting"  # External request to restart (e.g. cleared when running)
    FAILED = "failed"  # Task errored out
    UP_FOR_RETRY = "up_for_retry"  # Task failed but has retries left
    UP_FOR_RESCHEDULE = "up_for_reschedule"  # A waiting `reschedule` sensor
    UPSTREAM_FAILED = "upstream_failed"  # One or more upstream deps failed
    SKIPPED = "skipped"  # Skipped by branching or some other mechanism
    SENSING = "sensing"  # Smart sensor offloaded to the sensor DAG
    DEFERRED = "deferred"  # Deferrable operator waiting on a trigger

    def __str__(self) -> str:  # pylint: disable=invalid-str-returned
        return self.value


def get_job_state_from_airflow_state(state: str) -> str:
    if state in [AirflowState.FAILED]:
        return models.JobStatusClass.FAILED
    elif state in [AirflowState.RUNNING]:
        return models.JobStatusClass.IN_PROGRESS
    elif state in [AirflowState.SUCCESS]:
        return models.JobStatusClass.COMPLETED
    elif state in [AirflowState.RESTARTING, AirflowState.QUEUED]:
        return models.JobStatusClass.STARTING
    elif state in [AirflowState.SKIPPED]:
        return models.JobStatusClass.SKIPPED
    elif state in [AirflowState.DEFERRED]:
        return models.JobStatusClass.STOPPED
    else:
        return models.JobStatusClass.UNKNOWN


def generate_ownership_aspect(dag: DAG) -> List[models.OwnershipClass]:
    ownership = models.OwnershipClass(
        owners=[
            models.OwnerClass(
                owner=builder.make_user_urn(dag.owner),
                type=models.OwnershipTypeClass.DEVELOPER,
                source=models.OwnershipSourceClass(
                    type=models.OwnershipSourceTypeClass.SERVICE,
                    url=dag.filepath,
                ),
            )
        ],
        lastModified=models.AuditStampClass(
            time=0, actor=builder.make_user_urn("airflow")
        ),
    )
    return [ownership]


def generate_tags_aspect(dag: DAG) -> List[models.GlobalTagsClass]:
    tags = models.GlobalTagsClass(
        tags=[
            models.TagAssociationClass(tag=builder.make_tag_urn(tag))
            for tag in (dag.tags or [])
        ]
    )
    return [tags]


def process_dag(session: Session, dag: SerializedDAG, latest_runs: List[DagRun]):
    print("Processing DAG {dag}")
    flow_property_bag: Dict[str, str] = {
        key: repr(value) for (key, value) in dag.params.items()
    }
    base_url = conf.get("webserver", "BASE_URL")
    flow_url = f"{base_url}/tree?dag_id={dag.dag_id}"
    flow_urn = builder.make_data_flow_urn("airflow", dag.dag_id, config.cluster)
    latest_run: Optional[DagRun] = None

    flow_property_bag["fileloc"] = str(dag.fileloc)
    flow_property_bag["filepath"] = str(dag.filepath)
    flow_property_bag["tags"] = str(dag.tags)
    flow_property_bag["is_paused"] = str(dag.is_paused)
    flow_property_bag["concurrency"] = str(dag.concurrency)

    for run in latest_runs:
        if run.dag_id == dag.dag_id:
            flow_property_bag["state"] = str(run.get_state())
            flow_property_bag["execution_date"] = str(run.execution_date)
            # Not Airflow 1 compatible
            if not AIRFLOW_1:
                flow_property_bag["run_type"] = str(run.run_type)
            flow_property_bag["end_date"] = str(run.end_date)
            flow_property_bag["is_backfill"] = str(run.is_backfill)
            # Not Airflow 1 compatible
            if not AIRFLOW_1:
                flow_property_bag["creating_job_id"] = str(run.creating_job_id)
            flow_property_bag["conf"] = str(run.conf)
            flow_property_bag["external_trigger"] = str(run.external_trigger)
            # Not Airflow 1 compatible
            if not AIRFLOW_1:
                flow_property_bag["queued_at"] = str(run.queued_at)

            flow_property_bag["run_id"] = str(run.run_id)
            latest_run = run

    ownership_aspect: Optional[List[models.OwnershipClass]]
    if config.capture_ownership_info:
        ownership_aspect = generate_ownership_aspect(dag)
    tags_aspect: Optional[List[models.GlobalTagsClass]] = None
    if config.capture_tags_info:
        tags_aspect = generate_tags_aspect(dag)
    flow_mce = models.MetadataChangeEventClass(
        proposedSnapshot=models.DataFlowSnapshotClass(
            urn=flow_urn,
            aspects=[
                models.DataFlowInfoClass(
                    name=dag.dag_id,
                    description=f"{dag.description}\n\n{dag.doc_md or ''}",
                    customProperties=flow_property_bag,
                    externalUrl=flow_url,
                ),
                *ownership_aspect,
                *tags_aspect,
            ],
        )
    )
    yield flow_mce

    for task in dag.tasks:
        # exclude subdag operator tasks since these are not emitted, resulting in empty metadata
        print(f"Task: {task}")
        if task.params:
            if "datahub_ignore" in task.params:
                print(f"datahub_ignore is set for {task}, ignoring...")
                continue
        job_urn = builder.make_data_job_urn_with_flow(flow_urn, task.task_id)
        job_url = f"{base_url}/taskinstance/list/?flt1_dag_id_equals={dag.dag_id}&_flt_3_task_id={task.task_id}"
        job_property_bag: Dict[str, str] = {}
        upstream_tasks = []
        job_property_bag["email"] = str(task.email)
        job_property_bag["inlets"] = str(task.inlets)
        job_property_bag["outlets"] = str(task.outlets)
        job_property_bag["pool"] = str(task.pool)
        job_property_bag["queue"] = str(task.queue)
        job_property_bag["sla"] = str(task.sla)
        job_property_bag["trigger_rule"] = str(task.trigger_rule)
        job_property_bag["wait_for_downstream"] = str(task.wait_for_downstream)
        # Not Airflow 1 compatible
        if not AIRFLOW_1:
            job_property_bag["label"] = str(task.label)
        job_property_bag["depends_on_past"] = str(task.depends_on_past)
        job_property_bag["task_type"] = str(task.task_type)
        if task.task_type == "ExternalTaskSensor":
            print(f"external_dag_id:{getattr(task, 'external_dag_id', None)}")
            external_dag_id = getattr(task, "external_dag_id", None)
            if external_dag_id:
                job_property_bag["external_dag_id"] = str(external_dag_id)
                external_task_id = getattr(task, "external_task_id", None)
                external_task_ids = getattr(task, "external_task_ids", None)
                if external_task_id:
                    job_property_bag["external_task_id"] = str(external_task_id)
                else:
                    job_property_bag["external_task_id"] = str(external_task_ids)

        job_doc = (
            (task.doc or task.doc_md or task.doc_json or task.doc_yaml or task.doc_rst)
            if not AIRFLOW_1
            else None
        )
        job_state: Optional[str] = None

        if latest_run:
            run_tasks: Iterable[TaskInstance] = latest_run.get_task_instances()
            for ti in run_tasks:
                if ti.task_id == task.task_id:
                    job_property_bag["run_id"] = str(latest_run.run_id)
                    job_property_bag["current_state"] = str(ti.current_state(session))
                    job_property_bag["duration"] = str(ti.duration)
                    job_property_bag["start_date"] = str(ti.start_date)
                    job_property_bag["end_date"] = str(ti.end_date)
                    job_property_bag["execution_date"] = str(ti.execution_date)
                    job_property_bag["try_number"] = str(ti.try_number - 1)
                    job_property_bag["hostname"] = str(ti.hostname)
                    job_property_bag["max_tries"] = str(ti.max_tries)
                    # Not compatible with Airflow 1
                    if not AIRFLOW_1:
                        job_property_bag["external_executor_id"] = str(
                            ti.external_executor_id
                        )
                    job_property_bag["pid"] = str(ti.pid)
                    job_property_bag["state"] = str(ti.state)
                    job_property_bag["operator"] = str(ti.operator)
                    job_property_bag["priority_weight"] = str(ti.priority_weight)
                    job_property_bag["unixname"] = str(ti.unixname)
                    job_property_bag["log_url"] = ti.log_url
                    job_state = get_job_state_from_airflow_state(str(ti.state))

        # Set Upstreams
        for task_id in task.upstream_task_ids:
            upstream_tasks.append(
                builder.make_data_job_urn_with_flow(flow_urn, task_id)
            )

        job_mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataJobSnapshotClass(
                urn=job_urn,
                aspects=[
                    models.DataJobInfoClass(
                        name=task.task_id,
                        type=models.AzkabanJobTypeClass.COMMAND,
                        description=job_doc,
                        status=job_state,
                        customProperties=job_property_bag,
                        externalUrl=job_url,
                    ),
                    models.DataJobInputOutputClass(
                        inputDatasets=[],
                        outputDatasets=[],
                        inputDatajobs=upstream_tasks,
                    ),
                    *ownership_aspect,
                    *tags_aspect,
                ],
            )
        )
        yield job_mce


def emit_mces(emitter: DatahubRestEmitter, mces: List[MetadataChangeEvent]) -> None:

    for mce in mces:
        emitter.emit_mce(mce)


def ingest_airflow_dags():
    with create_session() as session:
        # sdm = session.query(DagBag).from_statement(text("select * from dagbag")).all()
        runs: List[DagRun] = DagRun.get_latest_runs(session)
        mces = []
        for dag in SerializedDagModel.read_all_dags().values():
            for mce in process_dag(session, dag, runs):
                mces.append(mce)
        # mces = [*mces]
        connection = BaseHook.get_connection(config.datahub_conn_id)
        if connection.conn_type == "http":
            emitter = DatahubRestEmitter(
                connection.host,
                connection.password,
                connection.extra_dejson.get("timeout_sec", 10),
            )
            emit_mces(emitter, mces)
        else:
            datahub_hook = DatahubGenericHook(config.datahub_conn_id)
            datahub_hook.emit_mces(mces)


config: AirflowIngestConfig

with DAG(
    "datahub_airflow_ingest",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    schedule_interval='@daily',
    catchup=False,  # enable if you don't want historical dag runs to run
) as dag:
    my_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(my_dir, "config.yaml")
    config_dict = load_config_file(config_file)
    config = AirflowIngestConfig.parse_obj(config_dict)
    t1 = PythonOperator(
        task_id="ingest_airflow_dags",
        python_callable=ingest_airflow_dags,
    )
