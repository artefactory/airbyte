#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests import codes, exceptions  # type: ignore[import]
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType

from .auth import BigqueryAuth
from .streams import BigqueryDatasets, BigqueryTables, BigqueryStream, BigqueryTable, BigqueryTableData
from .schema_helpers import SchemaHelpers

"""
This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""

# Source
class SourceBigquery(AbstractSource):
    logger = logging.getLogger("airbyte")
    streams_catalog: Iterable[Mapping[str, Any]] = []
    _auth: BigqueryAuth = None

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            self._auth = BigqueryAuth(config)
            # try reading first table from each dataset, to check the connectivity,
            for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                dataset_id = dataset.get("datasetReference")["datasetId"]
                for table_info in BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=self._auth).read_records(sync_mode=SyncMode.full_refresh):
                    table_id = table_info.get("tableReference")["tableId"]
                    BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=self._auth)
        except exceptions.HTTPError as error:
            error_msg = f"An error occurred: {error.response.text}"
            try:
                error_data = error.response.json()[0]
            except (KeyError, requests.exceptions.JSONDecodeError) as e:
                raise AirbyteTracedException(
                    internal_message=str(e),
                    failure_type=FailureType.system_error,
                    message=error_msg
                )
            else:
                error_code = error_data.get("errorCode")
                if error.response.status_code == codes.FORBIDDEN and error_code == "REQUEST_LIMIT_EXCEEDED":
                    logger.warn(f"API Call limit is exceeded. Error message: '{error_data.get('message')}'")
                    error_msg = "API Call limit is exceeded. Make sure that you have enough API allocation for your organization needs or retry later. For more information, see https://cloud.google.com/bigquery/quotas"
                    raise AirbyteTracedException(
                        internal_message=error_msg,
                        failure_type=FailureType.transient_error,
                        message=error_msg,
                    )
            return True, error_msg
        return True, None

    def discover(self, logger: AirbyteLogger, config) -> AirbyteCatalog:
        """
        Override to provide the dynamic schema generation capabilities,
        using resource available for authenticated user.

        Retrieve: Bases, Tables from each Base, generate JSON Schema for each table.
        """
        auth = self._auth or BigqueryAuth(config)
        # list all bases available for authenticated account
        for dataset in BigqueryDatasets(project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh):
            dataset_id = dataset.get("datasetReference")["datasetId"]
            # list and process each table under each base to generate the JSON Schema
            for table_info in BigqueryTables(dataset_id=dataset_id, project_id=config["project_id"], authenticator=auth).read_records(sync_mode=SyncMode.full_refresh):
                table_id = table_info.get("tableReference")["tableId"]
                table_obj = BigqueryTable(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=auth)

                for table in table_obj.read_records(sync_mode=SyncMode.full_refresh):
                    data_obj = BigqueryTableData(dataset_id=dataset_id, project_id=config["project_id"], table_id=table_id, authenticator=auth)
                    self.streams_catalog.append(
                        {
                            "stream_path": f"{table_obj.path()}",
                            "stream": SchemaHelpers.get_airbyte_stream(
                                f"{dataset_id}/{table_id}",
                                SchemaHelpers.get_json_schema(table),
                            ),
                            "table_name": table_id,
                            "table_data": data_obj
                        }
                    )
        return AirbyteCatalog(streams=[stream["stream"] for stream in self.streams_catalog])
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        pass
        