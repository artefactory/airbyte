from functools import wraps
from airbyte_protocol.models import FailureType
from jsonref import requests
from source_snowflake.snowflake_exceptions import emit_airbyte_error_message


def handle_no_permissions_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as error:
            error_message = str(error)
            error_code = error.response.status_code
            if int(error_code) == 422:
                try:
                    error_content = error.response.json()
                    no_permissions_or_no_table_message = "Object does not exist, or operation cannot be performed."
                    if "message" in error_content and no_permissions_or_no_table_message in error_content["message"]:
                        error_message = 'You do not have enough permission to read database/schema or database/schema does not exist'
                except ValueError:
                    pass
            emit_airbyte_error_message(error_message=error_message, failure_type=FailureType.config_error)
            raise requests.exceptions.HTTPError(error_message)
    return wrapper
