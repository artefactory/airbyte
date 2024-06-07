import pytest
import source_bigquery.auth

from oauth2client.service_account import ServiceAccountCredentials

@pytest.fixture(autouse=True)
def no_google_auth_check(mocker):
    source_bigquery.auth.RSASigner = mocker.MagicMock()
    source_bigquery.auth.ServiceAccountCredentials = mocker.MagicMock()
