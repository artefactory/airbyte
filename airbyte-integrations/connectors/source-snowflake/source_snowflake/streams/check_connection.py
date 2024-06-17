from typing import Iterable, Mapping
import requests
from source_snowflake.snowflake_exceptions import emit_airbyte_error_message
from source_snowflake.streams.snowflake_parent_stream import SnowflakeStream


class CheckConnectionStream(SnowflakeStream):
    DATABASE_NAME_COLUMN = "name"
    SCHEMA_NAME_COLUMN = "schema_name"

    def __init__(self, url_base, config, authenticator):
        super().__init__(authenticator=authenticator)
        self._url_base = url_base
        self._config = config

    @property
    def statement(self):

        """
        Assumptions:
            if we can see the table when showing schema, we assume we have access to the table (stream)
            We don't need to request the table in order to make sure it is working properly
            SHOW TABLES IN DATABASE statement does not include "system tables" in the dataset0
            if schema is provided by the use we replace the search of tables (streams) in database by search in shema
        """
        database = self.config["database"]
        schema = self.config.get('schema', "")
        if not schema:
            return f'SHOW TABLES IN DATABASE "{database}"'

        return f'SHOW TABLES IN SCHEMA "{database}"."{schema}"'

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        column_names_to_be_extracted_from_records = [self.DATABASE_NAME_COLUMN, self.SCHEMA_NAME_COLUMN]
        index_of_columns_from_names = self.get_index_of_columns_from_names(response_json, column_names_to_be_extracted_from_records)

        database_name_index = index_of_columns_from_names[self.DATABASE_NAME_COLUMN]
        schema_name_index = index_of_columns_from_names[self.SCHEMA_NAME_COLUMN]

        for record in response_json.get("data", []):
            try:
                yield {'schema': record[schema_name_index], 'table': record[database_name_index], }
            except Exception:
                error_message = 'Unexpected error while reading record'
                emit_airbyte_error_message(error_message)
