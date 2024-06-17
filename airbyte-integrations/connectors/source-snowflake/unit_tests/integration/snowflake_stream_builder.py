from airbyte_cdk.test.catalog_builder import ConfiguredAirbyteStreamBuilder


class SnowflakeStreamBuilder(ConfiguredAirbyteStreamBuilder):

    def with_cursor_field(self, cursor_field: list[str]) -> "SnowflakeStreamBuilder":
        self._stream["cursor_field"] = cursor_field
        return self
