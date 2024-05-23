from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_protocol.models import SyncMode

from .table_stream import TableStream
from .util_streams import StreamLauncher


class PushDownFilterStream(TableStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, namespace=None, **kwargs):
        kwargs['url_base'] = url_base
        kwargs['config'] = config
        kwargs['table_object'] = parent_stream.table_object
        kwargs['table_schema_stream'] = parent_stream.table_schema_stream
        TableStream.__init__(self, **kwargs)
        self._name = name
        self._namespace = namespace
        self._url_base = url_base
        self.config = config
        self._table_object = parent_stream.table_object
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
                                         **self._kwargs)

        post_response_iterable = stream_launcher.read_records(sync_mode=SyncMode.full_refresh)
        for post_response in post_response_iterable:
            if post_response:
                self.statement_handle = post_response['statementHandle']

    def __str__(self):
        return f"Current stream has this table object as constructor: {self.table_object} and as where clause: {self.where_clause}"
