#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch, AirbyteEntrypoint
from .source import SourceSnowflake

def run():
    _args = sys.argv[1:]
    catalog_path = AirbyteEntrypoint.extract_catalog(_args)
    config_path = AirbyteEntrypoint.extract_config(_args)
    state_path = AirbyteEntrypoint.extract_state(_args)
    source = SourceSnowflake()
    launch(source, sys.argv[1:])
