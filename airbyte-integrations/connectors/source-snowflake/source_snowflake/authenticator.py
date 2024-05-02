import base64
import hashlib
from datetime import timedelta, datetime, timezone
from typing import Text, Mapping, Any

from airbyte_cdk.sources.declarative.auth import JwtAuthenticator
from airbyte_cdk.sources.declarative.auth.jwt import JwtAlgorithm
from cryptography.hazmat.primitives._serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend


class SnowflakeJwtAuthenticator(JwtAuthenticator):
    TOKEN_DURATION = 55
    SNOWFLAKE_URL_SUFFIX = ".snowflakecomputing.com"
    HTTP_PREFIX = "https://"

    def get_auth_header(self) -> Mapping[str, Any]:
        """
        :return: A dictionary containing all the necessary headers to authenticate.
        """
        return {
            'Authorization': f"Bearer {self.token}"
        }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def from_config(cls, config):
        sub = cls.get_formatted_account(config['host'], config["credentials"]['user_name'])
        formatted_private_key = cls._format_private_key(config["credentials"]['private_key'])
        public_key_finger_print = cls.calculate_public_key_fingerprint(formatted_private_key, sub)
        kwargs = {"config": {},
                  "algorithm": JwtAlgorithm.RS256,
                  "iss": public_key_finger_print,
                  "sub": sub,
                  "secret_key": formatted_private_key,
                  'parameters': {},
                  'token_duration': cls.TOKEN_DURATION}
        return cls(**kwargs)

    @classmethod
    def get_formatted_account(cls, account: str, user_name: str) -> str:
        """
        Format the account and user_name to satisfy JWT token formatting
        """
        processed_account = account
        if account.endswith(cls.SNOWFLAKE_URL_SUFFIX):
            processed_account = processed_account.split(cls.SNOWFLAKE_URL_SUFFIX)[0]

        if account.startswith(cls.HTTP_PREFIX):
            processed_account = processed_account.split(cls.HTTP_PREFIX)[1]

        processed_account = processed_account.split('.')[0].upper()
        return f"{processed_account}.{user_name.upper()}"

    @classmethod
    def calculate_public_key_fingerprint(cls, private_key: Text, sub: Text) -> Text:
        """
        Given a private key in string format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        private_key_object = load_pem_private_key(private_key.encode(),
                                           None,
                                           default_backend())

        # Get the raw bytes of public key.
        public_key_raw = private_key_object.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = f"{sub}.SHA256:{base64.b64encode(sha256hash.digest()).decode('utf-8')}"

        return public_key_fp

    @classmethod
    def _format_private_key(cls, private_key: Text):
        start_private_key = "-----BEGIN PRIVATE KEY-----"
        end_private_key = "-----END PRIVATE KEY-----"
        content = private_key.split(start_private_key)[1].split(end_private_key)[0]
        content = content.replace(' ', '\n')
        return f'{start_private_key}{content}{end_private_key}'



