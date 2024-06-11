import os
import pytest
import airbyte_cdk.test.mock_http.request
import source_bigquery.auth

from typing import Any
from oauth2client.service_account import ServiceAccountCredentials

FAKE_ACCESS_TOKEN = "fake_access_token"

@pytest.fixture(autouse=True)
def no_google_auth_check(mocker, request):
    source_bigquery.auth.RSASigner = mocker.MagicMock()
    source_bigquery.auth.ServiceAccountCredentials = mocker.MagicMock(
        return_value=mocker.MagicMock(
            get_access_token=mocker.MagicMock(
                return_value=mocker.MagicMock(access_token=FAKE_ACCESS_TOKEN)
            )
        )
    )


@pytest.fixture(autouse=os.getenv("EXPLICIT_ERRORS", "false").lower() == "true")
def explicit_errors(mocker):
    try:
        old_matches = airbyte_cdk.test.mock_http.request.HttpRequest.matches

        def matches(self, other: Any) -> bool:
            temp_self = self
            temp_other = other


            temp_self._body = self._to_mapping(self._body)
            temp_other._body = other._to_mapping(other._body)

            print("-" * 100)
            for attr in ('_body', '_headers', '_parsed_url', '_query_params'):
                print(f"self.{attr} = {getattr(temp_self, attr)}")
                print(f"other.{attr} = {getattr(temp_other, attr)}")
                print("\n")
            print("-" * 100)

            return old_matches(self, other)

        airbyte_cdk.test.mock_http.request.HttpRequest.matches = matches
    except:
        pass