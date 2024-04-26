import base64
import hashlib
from datetime import timedelta, datetime, timezone
from typing import Text, Mapping, Any

from airbyte_cdk.sources.declarative.auth import JwtAuthenticator
from airbyte_cdk.sources.declarative.auth.jwt import JwtAlgorithm
from cryptography.hazmat.primitives._serialization import Encoding, PublicFormat


class SnowflakeJwtAuthenticator(JwtAuthenticator):

    def get_auth_header(self) -> Mapping[str, Any]:
        """
        :return: A dictionary containing all the necessary headers to authenticate.
        """
        print("token", self.token, flush=True)
        return {
            'Authorization': f"Bearer {self.token}"
        }
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def from_config(cls, config):
        kwargs = {"config": {},
                  "algorithm": JwtAlgorithm.RS256,
                  #"iss": cls.calculate_public_key_fingerprint(config["credentials"]['private_key']),
                  "sub": cls.get_formatted_account(config['host'], config["credentials"]['user_name']),
                  "secret_key": config["credentials"]['private_key'],
                  'parameters': {},
                  'token_duration': 60}
        return cls(**kwargs)

    @classmethod
    def get_formatted_account(cls, account: str, user_name: str) -> str:
        processed_account = account
        snow_flake_url_suffix = ".snowflakecomputing.com"
        http_prefix = "https://"
        if account.endswith(snow_flake_url_suffix):
            processed_account = processed_account.split(snow_flake_url_suffix)[0]
        if account.startswith(http_prefix):
            processed_account = processed_account.split(http_prefix)[1]
        processed_account = processed_account.split('.')[0].upper()
        return f"{processed_account}.{user_name.upper()}"

    @classmethod
    def calculate_public_key_fingerprint(cls, private_key: Text) -> Text:
        """
        Given a private key in PEM format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = 'SHA256:' + base64.b64encode(sha256hash.digest()).decode('utf-8')

        return public_key_fp


