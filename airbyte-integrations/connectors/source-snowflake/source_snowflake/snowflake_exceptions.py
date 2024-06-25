import traceback
from datetime import datetime
from airbyte_protocol.models import AirbyteMessage, AirbyteTraceMessage, TraceType, AirbyteErrorTraceMessage, FailureType
from airbyte_cdk.models import Type as AirbyteType


#######################################################
######### Connection errors
#######################################################

class IncorrectHostFormat(Exception):
    """
    This error happens when the user provides a url without the suffix ".snowflakecomputing.com"
    """
    pass


class DuplicatedPushDownFilterStreamNameError(Exception):
    """
    This error happens when the user selects CDC as update method and the latest refresh is higher than the retention time of the table
    Action:
        run a full refresh
        set an update time lower than retention time
    """
    pass


class InconsistentPushDownFilterParentStreamNameError(Exception):
    """
    This error happens when the user provides a push down filter with a wrong parent stream.
    Happens when:
        wrong spelling
        no permission to read parent stream name
    Action:
        verify format of parent stream: schema.table
        verify permissions on the table
    """

    def __init__(self, push_down_filters_without_consistent_parent):
        super().__init__()
        self.push_down_filters_without_consistent_parent = push_down_filters_without_consistent_parent


class UnknownUpdateMethodError(Exception):
    """
    Error occurs if the update_method is neither standard nor history
    """

    def __init__(self, unknown_update_method):
        super().__init__()
        self.unknown_update_method = unknown_update_method


class BadPrivateKeyFormatError(Exception):
    """
    This error happens when the user provides a url without the suffix ".snowflakecomputing.com"
    """
    pass


#######################################################
######### Stream Errors errors
#######################################################


class CursorFieldNotPresentInSchemaError(Exception):
    """
    Error occurs when cursor not present in schema
    Might happen if schema has evolved and cursor field set to previous value
    """
    pass


class SnowflakeTypeNotRecognizedError(Exception):
    """
    Error occurs if an unknown type is present in the schema sent by snowflake
    """
    pass


class MultipleCursorFieldsError(Exception):
    """
    Error occurs when the raw cursor field (list) provided is composed of more than one element
    """
    pass


#######################################################
######### CDC errors
#######################################################

class NotEnabledChangeTrackingOptionError(Exception):
    """
    This error happens when the user selects CDC as update method without having the change history option enabled
    Action:
        update table config with this command
            ALTER TABLE YOUR_TABLE SET CHANGE_TRACKING = TRUE;
        run full refresh on the table
    """

    def __init__(self, streams_without_change_tracking_enabled):
        super().__init__()
        self.streams_without_change_tracking_enabled = streams_without_change_tracking_enabled


class ChangeDataCaptureNotSupportedTypeGeographyError(Exception):
    """
    This error happens when the user selects CDC as update method and the user had the GEOGRAPHY type in this table
    Action:
        Update the type in the table otherwise CDC will not be available
    Note:
        It is a snowflake limitation
    """
    pass


class ChangeDataCaptureLookBackWindowUpdateFrequencyError(Exception):
    """
    This error happens when the user selects CDC as update method and the latest refresh is higher than the retention time of the table
    Action:
        run a full refresh
        set an update time lower than retention time
    """
    pass


class StartHistoryTimeNotSetError(Exception):
    """
    This error happens when the start time history is not set properly
    It should never happen
    """
    pass


def emit_airbyte_error_message(error_message, failure_type=FailureType.system_error):
    print(
        AirbyteMessage(
            type=AirbyteType.TRACE,
            trace=AirbyteTraceMessage(
                type=TraceType.ERROR,
                emitted_at=int(datetime.now().timestamp() * 1000),
                error=AirbyteErrorTraceMessage(
                    message=error_message,
                    stack_trace=traceback.format_exc(),
                    failure_type=failure_type
                ),
            ),
        ).json()
    )
