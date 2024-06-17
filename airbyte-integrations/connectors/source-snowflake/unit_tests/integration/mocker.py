# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import contextlib
import functools
from enum import Enum
from types import TracebackType
from typing import Callable, List, Optional, Union
from airbyte_cdk.test.mock_http import HttpMocker

import requests_mock
from airbyte_cdk.test.mock_http import HttpRequest, HttpRequestMatcher, HttpResponse


class SupportedHttpMethods(str, Enum):
    GET = "get"
    POST = "post"
    DELETE = "delete"


class CustomHttpMocker(HttpMocker):

    def __exit__(self, exc_type: Optional[BaseException], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self._mocker.__exit__(exc_type, exc_val, exc_tb)
        
        if exc_type == AssertionError:
            return False

        if exc_type == requests_mock.NoMockAddress :
            matchers_as_string = "\n\t".join(map(lambda matcher: str(matcher.request), self._matchers))
            error_to_raise =  ValueError(
                f"No matcher matches {exc_val.args[0]} with headers `{exc_val.request.headers}` "
                f"and body `{exc_val.request.body}`. "
                f"Matchers currently configured are:\n\t{matchers_as_string}."
            )
        
        try:
            self._validate_all_matchers_called()
        except ValueError as http_mocker_exception:
            # This seems useless as it catches ValueError and raises ValueError but without this, the prevailing error message in
            # the output is the function call that failed the assertion, whereas raising `ValueError(http_mocker_exception)`
            # like we do here provides additional context for the exception.
            raise ValueError(http_mocker_exception) from None

        if exc_type == AssertionError:
            return False
        
        

    # trying to type that using callables provides the error `incompatible with return type "_F" in supertype "ContextDecorator"`
    def __call__(self, f):  # type: ignore
        @functools.wraps(f)
        def wrapper(*args, **kwargs):  # type: ignore  # this is a very generic wrapper that does not need to be typed
            with self:
                assertion_error = None

                kwargs["http_mocker"] = self
                result = f(*args, **kwargs)
                
                return result

        return wrapper
