from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_protocol.models import SyncMode

from .table_stream import TableStream, TableChangeDataCaptureStream
from .util_streams import StreamLauncher, StreamLauncherChangeDataCapture


class PushDownFilterStream(TableStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, authenticator, namespace=None):
        super().__init__(url_base=url_base,
                         config=config,
                         table_object=parent_stream.table_object,
                         authenticator=authenticator)
        self._name = name
        self._namespace = namespace
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

    def set_statement_handle(self):
        if self.statement_handle:
            return

        stream_launcher = StreamLauncher(url_base=self.url_base,
                                         config=self.config,
                                         table_object=self.table_object,
                                         current_state=self.state,
                                         cursor_field=self.cursor_field,
                                         where_clause=self.where_clause,
                                         authenticator=self.authenticator)

        post_response_iterable = stream_launcher.read_records(sync_mode=SyncMode.full_refresh)
        for post_response in post_response_iterable:
            if post_response:
                json_post_response = post_response[0]
                self.statement_handle = json_post_response['statementHandle']

    def __str__(self):
        return f"Current stream has this table object as constructor: {self.table_object} and as where clause: {self.where_clause}"


class PushDownFilterChangeDataCaptureStream(TableChangeDataCaptureStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, authenticator, namespace=None):
        super().__init__(url_base=url_base,
                         config=config,
                         table_object=parent_stream.table_object,
                         authenticator=authenticator)
        self._name = name
        self._namespace = namespace
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

    def set_statement_handle(self):
        if self.statement_handle:
            return

        stream_launcher = StreamLauncherChangeDataCapture(url_base=self.url_base,
                                                          config=self.config,
                                                          table_object=self.table_object,
                                                          current_state=self.state,
                                                          cursor_field=self.cursor_field,
                                                          where_clause=self.where_clause,
                                                          authenticator=self.authenticator)

        post_response_iterable = stream_launcher.read_records(sync_mode=SyncMode.full_refresh)
        for post_response in post_response_iterable:
            if post_response:
                json_post_response = post_response[0]
                self.statement_handle = json_post_response['statementHandle']
