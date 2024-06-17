from .table_stream import TableStream, TableChangeDataCaptureStream


class PushDownFilterStream(TableStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, authenticator, namespace=None, time_zone_offset=None):
        super().__init__(url_base=url_base,
                         config=config,
                         table_object=parent_stream.table_object,
                         authenticator=authenticator,
                         time_zone_offset=time_zone_offset)
        self._name = name
        self._namespace = namespace
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

    def __str__(self):
        return f"Current stream has this table object as constructor: {self.table_object} and as where clause: {self.where_clause}"


class PushDownFilterChangeDataCaptureStream(TableChangeDataCaptureStream):

    def __init__(self, name, url_base, config, where_clause, parent_stream, authenticator, namespace=None, time_zone_offset=None):
        super().__init__(url_base=url_base,
                         config=config,
                         table_object=parent_stream.table_object,
                         authenticator=authenticator,
                         time_zone_offset=time_zone_offset)
        self._name = name
        self._namespace = namespace
        self.where_clause = where_clause
        self.table_schema_stream = parent_stream.table_schema_stream

    @property
    def name(self):
        return f"{self._name}"

