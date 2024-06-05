#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping, Union

import json
import httplib2
from google.auth.crypt import RSASigner
from oauth2client.service_account import ServiceAccountCredentials
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    Oauth2Authenticator,
    TokenAuthenticator,
)


SCOPES = ['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/cloud-platform.read-only', 'https://www.googleapis.com/auth/bigquery.readonly']

class BigqueryOAuth(TokenAuthenticator):
    """
    """
    def __init__(self, config):
        self._connector_config = config
        credentials_json = json.loads(config["credentials_json"], strict=False)
        self._credentials = ServiceAccountCredentials(credentials_json["client_email"], RSASigner.from_string(credentials_json["private_key"], \
                                                        credentials_json["private_key_id"]),\
                                                        scopes=SCOPES, \
                                                        private_key_id=credentials_json["private_key_id"], client_id=credentials_json["client_id"])
        self._http = httplib2.Http()
        self._credentials.authorize(self._http)
        self._token = str(self._credentials.get_access_token().access_token)
        super().__init__(self._token)

    @property
    def token(self) -> str:
        return f"{self._auth_method} {self.get_access_token()}"
    
    def get_access_token(self) -> str:
        """Retrieve new access and refresh token if the access token has expired.
        Returns:
            str: The current access_token, updated if it was previously expired.
        """
        if self._credentials.access_token_expired:
            self._credentials.refresh(self._http)
            self._token = str(self._credentials.get_access_token().access_token)
            # TODO: emit airbyte_cdk.sources.message
        return self._token
    

class BigqueryAuth:
    def __new__(cls, config: dict) -> Union[ServiceAccountCredentials, Oauth2Authenticator, TokenAuthenticator]:
        return BigqueryOAuth(config)
    