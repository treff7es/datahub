import logging
from collections import deque
from datetime import datetime
from typing import List, Optional, Tuple, Union

import cachetools
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Result

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    ExternalSystem,
    LinkedResourceSet,
    MissingExternalEntity,
    PlatformResourceRepository,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.unity_connection import UnityConnectionConfig
from datahub.metadata.com.linkedin.pegasus2avro.glossary import GlossaryNodeInfo
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub.metadata.urns import GlossaryNodeUrn, GlossaryTermUrn, TagUrn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.urn import Urn


class UnityTagSyncContext(BaseModel):
    metastore_id: str
    workspace_name: str


logger = logging.getLogger(__name__)


class UnityQueryExecutor:
    MAX_ERRORS_PER_HOUR = 100

    def __init__(self, config: UnityConnectionConfig) -> None:
        self.config: UnityConnectionConfig = config
        url = self.config.get_sql_alchemy_url()
        self.engine = create_engine(url, **self.config.get_options())
        self.error_threshold = self.MAX_ERRORS_PER_HOUR
        self.error_timestamps: deque = deque(maxlen=self.MAX_ERRORS_PER_HOUR * 2)
        self.last_error_cleanup = datetime.now()

    def _error_threshold_exceeded(self) -> bool:
        if len(self.error_timestamps) == 0:
            return False
        if (datetime.now() - self.last_error_cleanup).total_seconds() > 3600:
            self.last_error_cleanup = datetime.now()
            self.error_timestamps.clear()
        return len(self.error_timestamps) >= self.error_threshold

    def execute(self, query: str) -> Result:
        if self._error_threshold_exceeded():
            raise Exception("Error threshold exceeded")
        try:
            return self.engine.execute(query)
        except Exception as e:
            self.error_timestamps.append(datetime.now())
            raise e


class UnityTagId(BaseModel, ExternalEntityId):
    """
    A UnityTagId is a unique identifier for a Snowflake tag.
    """

    _RESOURCE_TYPE = "UnityTagId"
    metastore_id: str
    tag_name: str
    platform_instance: Optional[str]
    exists_in_unity: bool = False
    persisted: bool = False

    def __hash__(self) -> int:
        return hash(self.to_platform_resource_key().id)

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="databricks",
            resource_type=self._RESOURCE_TYPE,
            primary_key=f"{self.metastore_id}.{self.tag_name}",
            platform_instance=self.platform_instance,
        )

    @classmethod
    def from_datahub_urn(
            cls,
            urn: str,
            platform_resource_repository: PlatformResourceRepository,
            tag_sync_context: UnityTagSyncContext,
            graph: DataHubGraph,
    ) -> Optional["UnityTagId"]:
        """
        Creates a SnowflakeTagId from a DataHub URN.
        """
        # First we check if we already have a mapped platform resource for this
        # urn that is of the type SnowflakeTag
        # If we do, we can use it to create the SnowflakeTagId
        # Else, we need to generate a new SnowflakeTagId
        mapped_tags = [
            t
            for t in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (PlatformResourceSearchFields.RESOURCE_TYPE, cls._RESOURCE_TYPE),
                    (PlatformResourceSearchFields.SECONDARY_KEYS, urn),
                )
            )
        ]
        logger.info(
            f"Found {len(mapped_tags)} mapped tags for URN {urn}. {mapped_tags}"
        )
        if len(mapped_tags) > 0:
            if len(mapped_tags) > 1:
                logger.warning(f"Multiple mapped tags found for URN {urn}")
            platform_resource: PlatformResource = mapped_tags[0]
            if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
            ):
                unity_tag = UnityTag(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        UnityTag
                    ).dict()
                )
                unity_tag_id = unity_tag.id
                unity_tag_id.exists_in_unity = True
                unity_tag_id.persisted = True
                return unity_tag_id

        # Otherwise, we need to create a new UnityTagId
        new_unity_tag_id = cls.generate_tag_id(graph, tag_sync_context, urn)
        if new_unity_tag_id:
            # we then check if this tag has already been ingested as a platform
            # resource in the platform resource repository
            resource_key = platform_resource_repository.get(
                new_unity_tag_id.to_platform_resource_key()
            )
            if resource_key:
                logger.info(
                    f"Tag {new_unity_tag_id} already exists in platform resource repository with {resource_key}"
                )
                new_unity_tag_id.exists_in_unity = (
                    True  # TODO: Check if this is a safe assumption
                )
            return new_unity_tag_id
        raise ValueError(f"Unable to create SnowflakeTagId from DataHub URN: {urn}")

    @classmethod
    def generate_tag_id(
            cls, graph: DataHubGraph, tag_sync_context: UnityTagSyncContext, urn: str
    ) -> "UnityTagId":
        parsed_urn = Urn.from_string(urn)
        entity_type = parsed_urn.entity_type
        if entity_type == "tag":
            new_snowflake_tag_id = UnityTagId.from_datahub_tag(
                TagUrn.from_string(urn), tag_sync_context
            )
        elif entity_type == "glossaryTerm":
            new_snowflake_tag_id = UnityTagId.from_datahub_glossary_term(
                GlossaryTermUrn.from_string(urn), tag_sync_context, graph
            )
        else:
            raise ValueError(f"Unsupported entity type {entity_type} for URN {urn}")
        return new_snowflake_tag_id

    @classmethod
    def get_key_value_from_datahub_tag(
            cls, urn: Union[TagUrn, GlossaryTermUrn]
    ) -> Tuple[str, str]:
        tag_name = urn.name
        if ":" in tag_name:
            tag_name, value = tag_name.split(":", 1)
            return tag_name, value
        else:
            tag_name = tag_name
            return tag_name, ""

    @classmethod
    def from_datahub_tag(
            cls, tag_urn: TagUrn, tag_sync_context: UnityTagSyncContext
    ) -> "UnityTagId":
        tag_name, _ = cls.get_key_value_from_datahub_tag(tag_urn)

        return UnityTagId(
            tag_name=tag_name,
            metastore_id=tag_sync_context.metastore_id,
            platform_instance=tag_sync_context.workspace_name,
            exists_in_unity=False,
        )

    @classmethod
    def get_glossary_nodes(
            cls, glossary_node_urn: GlossaryNodeUrn, graph: DataHubGraph
    ) -> List[str]:
        node_info = graph.get_aspect(glossary_node_urn.urn(), GlossaryNodeInfo)
        if not node_info:
            return [glossary_node_urn.name]
        if node_info.parentNode:
            glossary_node_urn = GlossaryNodeUrn.from_string(node_info.parentNode)
            return cls.get_glossary_nodes(glossary_node_urn, graph) + [
                node_info.name if node_info.name else glossary_node_urn.name
            ]
        else:
            return [node_info.name if node_info.name else glossary_node_urn.name]

    @classmethod
    def get_glossary_term_name_from_id(
            cls, term_urn: GlossaryTermUrn, graph: DataHubGraph
    ) -> str:
        # needs resolution
        term_info = graph.get_aspect(term_urn.urn(), GlossaryTermInfoClass)
        if not term_info:
            raise ValueError(f"Term {term_urn} not found in graph.")

        logger.info(f"Resolved term {term_info}")

        parent_names: Optional[List[str]] = None
        if term_info and term_info.parentNode:
            glossary_node_urn = GlossaryNodeUrn.from_string(term_info.parentNode)
            parent_names = cls.get_glossary_nodes(glossary_node_urn, graph)

        if parent_names:
            parent_name = "__".join(parent_names)
            term_name = (
                    parent_name
                    + "__"
                    + (term_info.name if term_info.name else term_urn.name)
            )
        else:
            term_name = term_info.name if term_info.name else term_urn.name

        return term_name

    @classmethod
    def from_datahub_glossary_term(
            cls,
            glossary_term_urn: GlossaryTermUrn,
            tag_sync_context: UnityTagSyncContext,
            graph: DataHubGraph,
    ) -> "UnityTagId":
        term_name = cls.get_glossary_term_name_from_id(glossary_term_urn, graph)
        logger.info(f"Resolved term name {term_name}")
        return UnityTagId(
            metastore_id=tag_sync_context.metastore_id,
            tag_name=term_name,
            platform_instance=tag_sync_context.account_id,
        )


class UnityTag(BaseModel, ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: UnityTagId
    allowed_values: Optional[List[str]]

    def get_id(self) -> ExternalEntityId:
        return self.id

    def is_managed_by_datahub(self) -> bool:
        return self.managed_by_datahub

    def datahub_linked_resources(self) -> LinkedResourceSet:
        return self.datahub_urns

    def as_platform_resource(self) -> PlatformResource:
        return PlatformResource.create(
            key=self.id.to_platform_resource_key(),
            secondary_keys=[u for u in self.datahub_urns.urns],
            value=self,
        )

    @classmethod
    def get_from_unity(
            cls,
            unity_tag_id: UnityTagId,
            unity_query_executor: Optional[UnityQueryExecutor],
            platform_resource_repository: PlatformResourceRepository,
            check_tag_exists: bool = True,
    ) -> Optional["UnityTag"]:
        query = f"""
        select * from system.information_schema.table.tags  '{unity_tag_id.tag_name}'
        IN SCHEMA {unity_tag_id.database_name}.{unity_tag_id.schema_name}
        """
        try:
            if check_tag_exists:
                if not unity_query_executor:
                    raise ValueError(
                        "Snowflake query executor is required for this operation"
                    )

                result = unity_query_executor.execute(query).all()
                if not result or len(result) == 0:
                    return None

            # Search for linked DataHub URNs
            platform_resources = [
                r
                for r in platform_resource_repository.search_by_filter(
                    ElasticDocumentQuery.create_from(
                        (
                            PlatformResourceSearchFields.RESOURCE_TYPE,
                            UnityTagId._RESOURCE_TYPE,
                        ),
                        (
                            PlatformResourceSearchFields.PRIMARY_KEY,
                            f"{unity_tag_id.metastore_id}.{unity_tag_id.tag_name}",
                        ),
                    )
                )
            ]

            if len(platform_resources) == 1:
                platform_resource: PlatformResource = platform_resources[0]
                if (
                        platform_resource.resource_info
                        and platform_resource.resource_info.value
                ):
                    unity_tag = UnityTag(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            UnityTag
                        ).dict()
                    )
                    return unity_tag
            if len(platform_resources) > 1:
                logger.warning(
                    f"Multiple platform resources found for Unity tag {unity_tag_id}"
                )

            return cls(
                id=unity_tag_id,
                datahub_urns=LinkedResourceSet(urns=[]),
                managed_by_datahub=False,  # Assuming it's not managed by DataHub if it exists in Snowflake
                allowed_values=None,
            )
        except Exception as e:
            logger.error(f"Error fetching Unity tag {unity_tag_id}: {e}")
            return None


class SnowflakeSystem(ExternalSystem):
    def __init__(self, unity_connection_config: UnityConnectionConfig) -> None:
        super().__init__()
        self.unity_query_executor = UnityQueryExecutor(unity_connection_config)
        self.cached_entities: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1000, ttl=60 * 5
        )

    def exists(self, external_entity_id: ExternalEntityId) -> bool:
        return external_entity_id in self.cached_entities

    def get(
            self,
            external_entity_id: ExternalEntityId,
            platform_resource_repository: PlatformResourceRepository,
    ) -> Optional[ExternalEntity]:
        try:
            cached_entity = self.cached_entities[external_entity_id]
            if isinstance(cached_entity, MissingExternalEntity):
                return None
            return cached_entity
        except KeyError:
            external_entity = self._get_external_entity(
                external_entity_id, platform_resource_repository
            )
            if external_entity:
                self.cached_entities[external_entity_id] = external_entity
            else:
                # store a sentinel value to indicate that the entity does not
                # exist
                self.cached_entities[external_entity_id] = MissingExternalEntity(
                    external_entity_id
                )
            return external_entity

    def _get_external_entity(
            self,
            external_entity_id: ExternalEntityId,
            platform_resource_repository: PlatformResourceRepository,
    ) -> Optional[ExternalEntity]:
        if isinstance(external_entity_id, UnityTagId):
            return UnityTag.get_from_unity(
                external_entity_id,
                self.unity_query_executor,
                platform_resource_repository,
            )
        raise ValueError(
            f"Unsupported external entity id type: {type(external_entity_id)}"
        )