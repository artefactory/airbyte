#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.models import AirbyteErrorTraceMessage, AirbyteMessage, AirbyteTraceMessage, TraceType, Type

from .source import SourceBigquery


def run():
    _args = sys.argv[1:]
    catalog_path = AirbyteEntrypoint.extract_catalog(_args)
    config_path = AirbyteEntrypoint.extract_config(_args)
    state_path = AirbyteEntrypoint.extract_state(_args)

    source = SourceBigquery(
        SourceBigquery.read_catalog(catalog_path) if catalog_path else None,
        SourceBigquery.read_config(config_path) if config_path else None,
        SourceBigquery.read_state(state_path) if state_path else None,
    )
    if source:
        launch(source, sys.argv[1:])
