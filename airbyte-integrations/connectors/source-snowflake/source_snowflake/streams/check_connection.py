from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests

from source_snowflake.streams.snowflake_parent_stream import SnowflakeStream


class CheckConnectionStream(SnowflakeStream):

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
        records = response_json.get("data", [])
        yield from records

