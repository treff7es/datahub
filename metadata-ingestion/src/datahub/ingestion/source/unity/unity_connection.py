from typing import Optional, Dict, Any

from pydantic import SecretStr

from datahub.configuration import ConfigModel

from datahub.configuration.connection_resolver import auto_connection_resolver
from datahub.ingestion.source.sql.sql_config import make_sqlalchemy_uri

import pydantic

class UnityConnectionConfig(ConfigModel):
    # https://github.com/databricks/databricks-sqlalchemy/blob/main/README.md
    # Note: this config model is also used by the snowflake-usage source.

    _connection = auto_connection_resolver()

    options: dict = pydantic.Field(
        default_factory=dict,
        description="Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
    )

    scheme: str = "databricks"
    connect_args: Optional[Dict[str, Any]] = pydantic.Field(
        default=None,
        description="Connect args to pass to Snowflake SqlAlchemy driver",
        exclude=True,
    )
    token: Optional[str] = pydantic.Field(
        default=None,
        description="Set to the Databricks personal access token.",
    )

    http_path: Optional[str] = pydantic.Field(
        default=None,
        description="Set to HTTP Path value for your cluster or SQL warehouse..",
    )

    catalog: Optional[str] = pydantic.Field(
        default=None,
        description="Set to the target catalog in Unity Catalog",
    )

    def get_sql_alchemy_url(
            self,
            server_hostname: str,
            access_token: Optional[SecretStr] = None,
            catalog: Optional[str] = None,
            http_path: Optional[str] = None,
            schema: Optional[str] = None,
    ) -> str:
        if http_path is None:
            http_path = self.http_path
        if catalog is None:
            catalog = self.catalog
        if schema is None:
            schema = self.schema
        return make_sqlalchemy_uri(
            self.scheme,
            "token",
            access_token.get_secret_value() if access_token else None,
            at=server_hostname,
            db=None,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "http_path": http_path,
                    "catalog": catalog,
                    "schema": schema,
                }.items()
                if value
            },
        )
