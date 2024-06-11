import json
import logging
import tempfile
import traceback
from io import StringIO
from pathlib import Path as FilePath
from typing import Any, List, Mapping, Optional, Union, Dict, Tuple, TypeAlias

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.exception_handler import assemble_uncaught_exception
from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.sources import Source
from airbyte_protocol.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    Level,
    TraceType,
    Type,
)
from pydantic.error_wrappers import ValidationError
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, make_file


def discover(
    source: Source,
    config: Mapping[str, Any],
    expecting_exception: bool = False,
) -> EntrypointOutput:
    """
    config and state must be json serializable

    :param expecting_exception: By default if there is an uncaught exception, the exception will be printed out. If this is expected, please
        provide expecting_exception=True so that the test output logs are cleaner
    """
    log_capture_buffer = StringIO()
    stream_handler = logging.StreamHandler(log_capture_buffer)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(AirbyteLogFormatter())
    parent_logger = logging.getLogger("")
    parent_logger.addHandler(stream_handler)

    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        args = [
            "discover",
            "--config",
            make_file(tmp_directory_path / "config.json", config),
        ]
        args.append("--debug")
        source_entrypoint = AirbyteEntrypoint(source)
        parsed_args = source_entrypoint.parse_args(args)

        messages = []
        uncaught_exception = None
        try:
            for message in source_entrypoint.run(parsed_args):
                messages.append(message)
        except Exception as exception:
            if not expecting_exception:
                print("Printing unexpected error from entrypoint_wrapper")
                print("".join(traceback.format_exception(None, exception, exception.__traceback__)))
            uncaught_exception = exception

        captured_logs = log_capture_buffer.getvalue().split("\n")[:-1]

        parent_logger.removeHandler(stream_handler)

        return EntrypointOutput(messages + captured_logs, uncaught_exception)
