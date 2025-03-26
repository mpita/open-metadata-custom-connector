import traceback
from typing import Iterable, Optional

from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema

from metadata.generated.schema.entity.data.table import Column, DataType, Table, TableData

logger = ingestion_logger()


class MySimpleConnector(Source):
    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata
        super().__init__()

    def prepare(self):
        pass

    def test_connection(self):
        pass

    @classmethod
    def create(
        cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ) -> "MySimpleConnector":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata)
    
    def yield_create_request_database_service(self, config: WorkflowSource):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=config
            )
        )
    
    def _iter(self) -> Iterable[Either]:
        try:
            yield from self.yield_create_request_database_service(self.config)

            service_entity: DatabaseService = self.metadata.get_by_name(
                entity=DatabaseService, fqn=self.config.serviceName
            )

            yield Either(
                right=CreateDatabaseRequest(
                    name="custom-database",
                    service=service_entity.fullyQualifiedName,
                )
            )

            database_entity = self.metadata.get_by_name(
                entity=Database, 
                fqn=f"{self.config.serviceName}.custom-database"
            )
            
            yield Either(
                right=CreateDatabaseSchemaRequest(
                    name="custom-schema",
                    database=database_entity.fullyQualifiedName
                )
            )

            schema_entity = self.metadata.get_by_name(
                entity=DatabaseSchema,
                fqn=f"{self.config.serviceName}.custom-database.custom-schema"
            )
            
            yield Either(
                right=CreateTableRequest(
                    name="custom-table",
                    databaseSchema=schema_entity.fullyQualifiedName,
                    columns=[
                        Column(
                            name="id",
                            dataType=DataType.INT,
                            description="ID column"
                        ),
                        Column(
                            name="name",
                            dataType=DataType.STRING,
                            description="Name column"
                        ),
                        Column(
                            name="created_at",
                            dataType=DataType.DATETIME,
                            description="Creation timestamp"
                        )
                    ]
                )
            )

            table_entity = self.metadata.get_by_name(
                entity=Table,
                fqn=f"{self.config.serviceName}.custom-database.custom-schema.custom-table"
            )

            sample_data = [
                {"id": 1, "name": "John Doe", "created_at": "2025-03-26T10:00:00Z"},
                {"id": 2, "name": "Jane Smith", "created_at": "2025-03-26T11:30:00Z"},
                {"id": 3, "name": "Bob Johnson", "created_at": "2025-03-26T14:15:00Z"}
            ]

            # Update the table with sample data
            self.metadata.ingest_table_sample_data(
                table=table_entity,
                sample_data=TableData(
                    columns=["id", "name", "created_at"],
                    rows=[[row["id"], row["name"], row["created_at"]] for row in sample_data]
                )
            )

        except Exception as e:
            logger.error("Error creating schema or table")
            logger.error(f"Error: {e}")
            yield Either(
                left=StackTraceError(
                    name="My Error",
                    error="Demoing one error",
                    stackTrace=traceback.format_exc(),
                )
            )

    def close(self):
        pass