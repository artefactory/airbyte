#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping, Union

import requests
import httplib2
from google.auth.crypt import RSASigner
from oauth2client import GOOGLE_TOKEN_URI
from oauth2client.service_account import ServiceAccountCredentials, _JWTAccessCredentials
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    BasicHttpAuthenticator,
    Oauth2Authenticator,
    TokenAuthenticator,
)
from airbyte_cdk.utils import AirbyteTracedException

# class BigqueryServiceAccountCredentials(ServiceAccountCredentials):


class BigqueryOAuth(Oauth2Authenticator):
    """
    https://airtable.com/developers/web/api/oauth-reference#token-expiry-refresh-tokens
    """

    def build_refresh_request_headers(self) -> Mapping[str, Any]:
        """
        https://airtable.com/developers/web/api/oauth-reference#token-refresh-request-headers
        """
        return {
            "Authorization": BasicHttpAuthenticator(self.get_client_id(), self.get_client_secret()).token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def build_refresh_request_body(self) -> Mapping[str, Any]:
        """
        https://airtable.com/developers/web/api/oauth-reference#token-refresh-request-body
        """
        return {
            "grant_type": self.get_grant_type(),
            "refresh_token": self.get_refresh_token(),
        }

    def _get_refresh_access_token_response(self) -> Mapping[str, Any]:
        response = requests.request(
            method="POST",
            url=self.get_token_refresh_endpoint(),
            data=self.build_refresh_request_body(),
            headers=self.build_refresh_request_headers(),
        )
        content = response.json()
        if response.status_code == 400 and content.get("error") == "invalid_grant":
            raise AirbyteTracedException(
                internal_message=content.get("error_description"),
                message="Refresh token is invalid or expired. Please re-authenticate to restore access to Airtable.",
                failure_type=FailureType.config_error,
            )
        response.raise_for_status()
        return content


class BigqueryAuth:
    def __new__(cls, config: dict) -> Union[ServiceAccountCredentials, BigqueryOAuth, Oauth2Authenticator]:       
        # for new oauth configs
        credentials_json = config["credentials_json"]
        credentials = ServiceAccountCredentials(credentials_json["client_email"], RSASigner.from_string(credentials_json["private_key"], credentials_json["private_key_id"]),\
                                                scopes=['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/devstorage.full_control'], \
                                                private_key_id=credentials_json["private_key_id"], client_id=credentials_json["client_id"])
        # print(credentials.client_id)
        # print(credentials.get_access_token())
        h = httplib2.Http()
        h = credentials.authorize(h)
        r = credentials.refresh(h)
        # Oauth2Authenticator()
        # print(credentials.client_secret)
        # print(credentials.refresh_token)
        # print("refresh ", r)
        # print(credentials.serialization_data)
        # print(credentials.token_uri)
        # # print(credentials.access_token)
        # print(credentials.token_expiry)
        # print(credentials.access_token_expired)
        sc = credentials.create_scoped(scopes=['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/cloud-platform.read-only'])
        auth = TokenAuthenticator(token=credentials.get_access_token())
        # print("scope")
        token = sc.get_access_token()
        # print(sc.token_expiry)
        # print(sc.client_secret)
        # print(sc.refresh_token)
        # print(sc.get_client_secret())
        # auth = Oauth2Authenticator(token_refresh_endpoint=credentials_json["token_uri"], client_id=credentials_json["client_id"], client_secret=credentials_json["private_key"],refresh_token=token)
        # print(str(token.access_token))
        access_token = str(token.access_token)
        auth = TokenAuthenticator(token=access_token)
        return auth
        # if credentials["auth_method"] == "oauth2.0":
        #     return BigqueryOAuth(config, "https://airtable.com/oauth2/v1/token")
        # elif credentials["auth_method"] == "api_key":
        #     return TokenAuthenticator(token=credentials["api_key"])
