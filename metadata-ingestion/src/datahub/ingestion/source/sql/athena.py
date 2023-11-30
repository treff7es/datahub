import json
import logging
import re
import typing
from builtins import RuntimeError
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast, Set

import pydantic
from pyathena.common import BaseCursor
from pyathena.model import AthenaTableMetadata
from pyathena.sqlalchemy_athena import AthenaRestDialect
from sqlalchemy import create_engine, exc, inspect, text, types
from sqlalchemy.engine import reflection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import TypeEngine
from sqlalchemy_bigquery import STRUCT

from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp_builder import ContainerKey, DatabaseKey
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_lowercase_urns
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig, AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    register_custom_type,
    SqlWorkUnit,
    SQLSourceReport,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig, make_sqlalchemy_uri
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata._schema_classes import SchemaMetadataClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import MapTypeClass, RecordTypeClass
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column
from datahub.utilities.sqlalchemy_type_converter import (
    MapType,
    get_schema_fields_for_sqlalchemy_column,
)
from datahub.utilities.sqlglot_lineage import sqlglot_lineage

logger = logging.getLogger(__name__)

assert STRUCT, "required type modules are not available"
register_custom_type(STRUCT, RecordTypeClass)
register_custom_type(MapType, MapTypeClass)


class CustomAthenaRestDialect(AthenaRestDialect):
    """Custom definition of the Athena dialect.

    Custom implementation that allows to extend/modify the behavior of the SQLalchemy
    dialect that is used by PyAthena (which is the library that is used by DataHub
    to extract metadata from Athena).
    This dialect can then be used by the inspector (see get_inspectors()).

    """

    # regex to identify complex types in DDL strings which are embedded in `<>`.
    _complex_type_pattern = re.compile(r"(<.+>)")

    @typing.no_type_check
    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        # This method was backported from PyAthena v3.0.7 to allow to retrieve the view definition
        # from Athena. This is required until we support sqlalchemy > 2.0
        # https://github.com/laughingman7743/PyAthena/blob/509dd37d0fd15ad603993482cc47b8549b82facd/pyathena/sqlalchemy/base.py#L1118
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name  # type: ignore
        query = f"""SHOW CREATE VIEW "{schema}"."{view_name}";"""
        try:
            res = connection.scalars(text(query))
        except exc.OperationalError as e:
            raise exc.NoSuchTableError(f"{schema}.{view_name}") from e
        else:
            return "\n".join([r for r in res])

    @typing.no_type_check
    def _get_column_type(
        self, type_: Union[str, Dict[str, Any]]
    ) -> TypeEngine:  # noqa: C901
        """Derives the data type of the Athena column.

        This method is overwritten to extend the behavior of PyAthena.
        Pyathena is not capable of detecting complex data types, e.g.,
        arrays, maps, or, structs (as of version 2.25.2).
        The custom implementation extends the functionality by the above-mentioned data types.
        """

        # Originally, this method only handles `type_` as a string
        # With the workaround used below to parse DDL strings for structs,
        # `type` might also be a dictionary
        if isinstance(type_, str):
            match = self._pattern_column_type.match(type_)
            if match:
                type_name = match.group(1).lower()
                type_meta_information = match.group(2)
            else:
                type_name = type_.lower()
                type_meta_information = None
        elif isinstance(type_, dict):
            # this occurs only when a type parsed as part of a STRUCT is passed
            # in such case type_ is a dictionary whose type can be retrieved from the attribute
            type_name = type_.get("type", None)
            type_meta_information = None
        else:
            raise RuntimeError(f"Unsupported type definition: {type_}")

        args = []

        if type_name in ["array"]:
            detected_col_type = types.ARRAY

            # here we need to account again for two options how `type_` is passed to this method
            # first, the simple array definition as a DDL string (something like array<string>)
            # this is always the case when the array is not part of a complex data type (mainly STRUCT)
            # second, the array definition can also be passed in form of dictionary
            # this is the case when the array is part of a complex data type
            if isinstance(type_, str):
                # retrieve the raw name of the data type as a string
                array_type_raw = self._complex_type_pattern.findall(type_)[0][
                    1:-1
                ]  # array type without enclosing <>
                # convert the string name of the data type into a SQLalchemy type (expected return)
                array_type = self._get_column_type(array_type_raw)
            elif isinstance(type_, dict):
                # retrieve the data type of the array items and
                # transform it into a SQLalchemy type
                array_type = self._get_column_type(type_["items"])
            else:
                raise RuntimeError(f"Unsupported array definition: {type_}")

            args = [array_type]

        elif type_name in ["struct", "record"]:
            # STRUCT is not part of the SQLalchemy types selection
            # but is provided by another official SQLalchemy library and
            # compatible with the other SQLalchemy types
            detected_col_type = STRUCT

            if isinstance(type_, dict):
                # in case a struct as part of another struct is passed
                # it is provided in form of a dictionary and
                # can simply be used for the further processing
                struct_type = type_
            else:
                # this is the case when the type definition of the struct is passed as a DDL string
                # therefore, it is required to parse the DDL string
                # here a method provided in another Datahub source is used so that the parsing
                # doesn't need to be implemented twice
                # `get_avro_schema_for_hive_column` accepts a DDL description as column type and
                # returns the parsed data types in form of a dictionary
                schema = get_avro_schema_for_hive_column(
                    hive_column_name=type_name, hive_column_type=type_
                )

                # the actual type description needs to be extracted
                struct_type = schema["fields"][0]["type"]

            # A STRUCT consist of multiple attributes which are expected to be passed as
            # a list of tuples consisting of name data type pairs. e.g., `('age', Integer())`
            # See the reference:
            # https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/sqlalchemy_bigquery/_struct.py#L53
            #
            # To extract all of them, we simply iterate over all detected fields and
            # convert them to SQLalchemy types
            struct_args = []
            for field in struct_type["fields"]:
                struct_args.append(
                    (
                        field["name"],
                        self._get_column_type(field["type"]["type"])
                        if field["type"]["type"] not in ["record", "array"]
                        else self._get_column_type(field["type"]),
                    )
                )

            args = struct_args

        elif type_name in ["map"]:
            # Instead of SQLalchemy's TupleType the custom MapType is used here
            # which is just a simple wrapper around TupleType
            detected_col_type = MapType

            # the type definition for maps looks like the following: key_type:val_type (e.g., string:string)
            key_type_raw, value_type_raw = type_meta_information.split(",")

            # convert both type names to actual SQLalchemy types
            args = [
                self._get_column_type(key_type_raw),
                self._get_column_type(value_type_raw),
            ]
        # by using get_avro_schema_for_hive_column() for parsing STRUCTs the data type `long`
        # can also be returned, so we need to extend the handling here as well
        elif type_name in ["bigint", "long"]:
            detected_col_type = types.BIGINT
        elif type_name in ["decimal"]:
            detected_col_type = types.DECIMAL
            precision, scale = type_meta_information.split(",")
            args = [int(precision), int(scale)]
        else:
            return super()._get_column_type(type_name)
        return detected_col_type(*args)


class AthenaConfig(SQLCommonConfig):
    aws_config: Optional[AwsConnectionConfig] = pydantic.Field(
        default=None, description="AWS configuration"
    )

    scheme: str = "awsathena+rest"
    username: Optional[str] = pydantic.Field(
        default=None,
        description="Username credential. If not specified, detected with boto3 rules. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html",
    )
    password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, description="Same detection scheme as username"
    )
    database: Optional[str] = pydantic.Field(
        default=None,
        description="The athena database to ingest from. If not set it will be autodetected",
    )
    aws_region: str = pydantic.Field(
        description="Aws region where your Athena database is located"
    )
    aws_role_arn: Optional[str] = pydantic.Field(
        default=None,
        description="AWS Role arn for Pyathena to assume in its connection",
    )
    aws_role_assumption_duration: int = pydantic.Field(
        default=3600,
        description="Duration to assume the AWS Role for. Maximum of 43200 (12 hours)",
    )
    s3_staging_dir: Optional[str] = pydantic.Field(
        default=None,
        deprecated=True,
        description="[deprecated in favor of `query_result_location`] S3 query location",
    )
    work_group: str = pydantic.Field(
        description="The name of your Amazon Athena Workgroups"
    )
    catalog_name: str = pydantic.Field(
        default="awsdatacatalog",
        description="Athena Catalog Name",
    )

    query_result_location: str = pydantic.Field(
        description="S3 path to the [query result bucket](https://docs.aws.amazon.com/athena/latest/ug/querying.html#query-results-specify-location) which should be used by AWS Athena to store results of the"
        "queries executed by DataHub."
    )

    include_table_lineage = pydantic.Field(
        default=False,
        description="Whether to include table lineage in the ingestion. "
        "This requires to have the table lineage feature enabled.",
    )
    include_view_lineage = pydantic.Field(
        default=True,
        description="Whether to include view lineage in the ingestion. "
        "This requires to have the view lineage feature enabled.",
    )
    usage: BaseUsageConfig = pydantic.Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )
    include_usage_statistics: bool = pydantic.Field(
        default=False,
        description="Generate usage statistic.",
    )

    # overwrite default behavior of SQLAlchemyConfing
    include_views: Optional[bool] = pydantic.Field(
        default=True, description="Whether views should be ingested."
    )

    _s3_staging_dir_population = pydantic_renamed_field(
        old_name="s3_staging_dir",
        new_name="query_result_location",
        print_warning=True,
    )

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username or "",
            self.password.get_secret_value() if self.password else None,
            f"athena.{self.aws_region}.amazonaws.com:443",
            self.database,
            uri_opts={
                # as an URI option `s3_staging_dir` is still used due to PyAthena
                "s3_staging_dir": self.query_result_location,
                "work_group": self.work_group,
                "catalog_name": self.catalog_name,
                "role_arn": self.aws_role_arn,
                "duration_seconds": str(self.aws_role_assumption_duration),
            },
        )


class AthenaReport(SQLSourceReport, BaseTimeWindowReport):
    num_queries_parsed: int = 0
    num_view_ddl_parsed: int = 0
    num_table_parse_failures: int = 0


@platform_name("Athena")
@support_status(SupportStatus.CERTIFIED)
@config_class(AthenaConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration. Profiling uses sql queries on whole table which can be expensive operation.",
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported for S3 tables")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
class AthenaSource(SQLAlchemySource):
    """
    This plugin supports extracting the following metadata from Athena
    - Tables, schemas etc.
    - Lineage for S3 tables.
    - Profiling when enabled.
    """

    def __init__(self, config: AthenaConfig, ctx):
        super().__init__(config, ctx, "athena")
        self.report: AthenaReport = AthenaReport()
        self.cursor: Optional[BaseCursor] = None

    @classmethod
    def create(cls, config_dict, ctx):
        config = AthenaConfig.parse_obj(config_dict)
        cls.builder: SqlParsingBuilder = SqlParsingBuilder(
            usage_config=config.usage if config.include_usage_statistics else None,
            generate_lineage=True,
            generate_usage_statistics=config.include_usage_statistics,
            generate_operations=config.usage.include_operational_stats,
        )
        return cls(config, ctx)

    # overwrite this method to allow to specify the usage of a custom dialect
    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        # set custom dialect to be used by the inspector
        engine.dialect = CustomAthenaRestDialect()
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        schema, _view = dataset_identifier.split(".", 1)
        return None, schema

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        if not self.cursor:
            self.cursor = cast(BaseCursor, inspector.engine.raw_connection().cursor())
            assert self.cursor

        metadata: AthenaTableMetadata = self.cursor.get_table_metadata(
            table_name=table, schema_name=schema
        )
        description = metadata.comment
        custom_properties: Dict[str, str] = {}
        custom_properties["partition_keys"] = json.dumps(
            [
                {
                    "name": partition.name,
                    "type": partition.type,
                    "comment": partition.comment if partition.comment else "",
                }
                for partition in metadata.partition_keys
            ]
        )
        for key, value in metadata.parameters.items():
            custom_properties[key] = value if value else ""

        custom_properties["create_time"] = (
            str(metadata.create_time) if metadata.create_time else ""
        )
        custom_properties["last_access_time"] = (
            str(metadata.last_access_time) if metadata.last_access_time else ""
        )
        custom_properties["table_type"] = (
            metadata.table_type if metadata.table_type else ""
        )

        location: Optional[str] = custom_properties.get("location", None)
        if location is not None:
            if location.startswith("s3://"):
                location = make_s3_urn(location, self.config.env)
            else:
                logging.debug(
                    f"Only s3 url supported for location. Skipping {location}"
                )
                location = None

        return description, custom_properties, location

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        # In Athena the schema is the database and database is not existing
        return []

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.get_database_container_key(
            db_name=database, schema=schema
        )

        yield from gen_database_container(
            database=database_container_key.database,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def get_database_container_key(self, db_name: str, schema: str) -> DatabaseKey:
        # Because our overridden get_allowed_schemas method returns db_name as the schema name,
        # the db_name and schema here will be the same. Hence, we just ignore the schema parameter.
        # Based on community feedback, db_name only available if it is explicitly specified in the connection string.
        # If it is not available then we should use schema as db_name

        if not db_name:
            db_name = schema

        return gen_database_key(
            db_name,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def add_table_to_schema_container(
        self,
        dataset_urn: str,
        db_name: str,
        schema: str,
        schema_container_key: Optional[ContainerKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=self.get_database_container_key(db_name, schema),
        )

    # It seems like database/schema filter in the connection string does not work and this to work around that
    def get_schema_names(self, inspector: Inspector) -> List[str]:
        athena_config = typing.cast(AthenaConfig, self.config)
        schemas = inspector.get_schema_names()
        if athena_config.database:
            return [schema for schema in schemas if schema == athena_config.database]
        return schemas

    # Overwrite to modify the creation of schema fields
    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict,
        pk_constraints: Optional[dict] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        fields = get_schema_fields_for_sqlalchemy_column(
            column_name=column["name"],
            column_type=column["type"],
            description=column.get("comment", None),
            nullable=column.get("nullable", True),
            is_part_of_key=True
            if (
                pk_constraints is not None
                and isinstance(pk_constraints, dict)
                and column["name"] in pk_constraints.get("constrained_columns", [])
            )
            else False,
        )

        return fields

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Add all schemas to the schema resolver
        # Sql parser operates on lowercase urns so we need to lowercase the urns
        for wu in auto_lowercase_urns(super().get_workunits_internal()):
            urn = wu.get_urn()
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self.schema_resolver.add_schema_metadata(urn, schema_metadata)
            yield wu

        urns = self.schema_resolver.get_urns()
        if self.config.include_table_lineage or self.config.include_usage_statistics:
            # self.report.report_ingestion_stage_start("audit log extraction")
            yield from self.get_audit_log_mcps(urns=urns)

        yield from self.builder.gen_workunits()

    def get_audit_log_mcps(self, urns: Set[str]) -> Iterable[MetadataWorkUnit]:
        if not self.config.aws_config:
            raise RuntimeError("AWS config is required for audit log extraction")
        client = self.config.aws_config.get_athena_client()
        ids = []
        response = client.list_query_executions(MaxResults=50)
        process_more = False
        if response["QueryExecutionIds"]:
            process_more = True
        while process_more:
            ids.extend(response["QueryExecutionIds"])
            logger.info(f"Retrieved {len(ids)} query execution ids")
            qe_response = client.batch_get_query_execution(
                QueryExecutionIds=response["QueryExecutionIds"]
            )
            for qe in qe_response["QueryExecutions"]:
                if qe["Status"]["State"] == "SUCCEEDED":
                    self.report.num_queries_parsed += 1
                    if self.report.num_queries_parsed % 50 == 0:
                        logger.info(f"Parsed {self.report.num_queries_parsed} queries")

                    yield from self.gen_lineage_from_query(
                        query=qe["Query"],
                        default_database=qe["QueryExecutionContext"]["Database"],
                        timestamp=qe["Status"]["SubmissionDateTime"],
                        user="random_user",
                        urns=urns,
                    )

            if response.get("NextToken") is None:
                process_more = False
            else:
                response = client.list_query_executions(
                    MaxResults=50, NextToken=response.get("NextToken")
                )

    def gen_lineage_from_query(
        self,
        query: str,
        default_database: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        user: Optional[str] = None,
        view_urn: Optional[str] = None,
        urns: Optional[Set[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        result = sqlglot_lineage(
            # With this clever hack we can make the query parser to not fail on queries with CASESPECIFIC
            sql=query,
            schema_resolver=self.schema_resolver,
            default_db=None,
            default_schema=default_database
            if default_database
            else self.config.default_db,
        )
        if result.debug_info.table_error:
            logger.debug(
                f"Error parsing table lineage ({view_urn}):\n{result.debug_info.table_error}"
            )
            self.report.num_table_parse_failures += 1
        else:
            yield from self.builder.process_sql_parsing_result(
                result,
                query=query,
                is_view_ddl=view_urn is not None,
                query_timestamp=timestamp,
                user=f"urn:li:corpuser:{user}",
                include_urns=urns,
            )

    def close(self):
        if self.cursor:
            self.cursor.close()
        super().close()
