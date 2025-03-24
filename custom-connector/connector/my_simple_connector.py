import traceback

from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)


class MySimpleConnector(Source):
    def __init__(self, config, metadata):
        self.config = config
        self.metadata = metadata
        self.data = None

        self.service_connection = config.serviceConnection.root.config

        super().__init__()
    
    def prepare(self):
        # Método obligatorio aunque no hagas nada
        pass

    def test_connection(self):
        # Método opcional, solo si configurás test en el YAML
        pass

    @classmethod
    def create(cls, config_dict, metadata, pipeline_name = None):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata)

    # @classmethod
    # def create(cls, config_dict: dict, metadata_config, pipeline_name: str = None):
    #     config = CustomDatabaseConnection.parse_obj(config_dict["serviceConnection"]["config"])
    #     return cls(config, metadata_config)

    def _iter(self):
        # Simula la creación de una tabla ficticia
        try:
            yield Either(
                right=CreateTableRequest(
                    name="my_test_table",
                    tableType=TableType.Regular,
                    databaseSchema="default",  # Asegurate que exista o usa uno válido
                    service=EntityReference(
                        id=self.context["database_service"].id,
                        type="databaseService"
                    )
                )
            )
        except Exception as e:
            print(f"Error: {e}")
            yield Either(
                left=StackTraceError(
                    name="My Error",
                    error="Demoing one error",
                    stackTrace=traceback.format_exc(),
                )
            )

    def close(self):
        pass