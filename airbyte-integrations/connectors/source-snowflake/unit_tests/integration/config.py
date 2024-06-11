from datetime import datetime
from typing import Any, Dict
import uuid


class ConfigBuilder:
    def __init__(self, jwt_token: str, host: str, schema: str, database: str, role: str, warehouse: str) -> None:
        self._push_down_filters = []
        self._config: Dict[str, Any] = {
            "credentials": {
                "auth_type": "JWT Token",
                "user_name": "ConfigBuilder default username",
                "private_key": "-----BEGIN PRIVATE KEY-----key-----END PRIVATE KEY-----",

            },
            "host": host,
            "role": role,
            "schema": schema,
            "database": database,
            "warehouse": warehouse,
            "replication_method": {
                "method": "standard"
            }
        }

    def with_push_down_filter(self, push_down_filter: dict):
        self._push_down_filters.append(push_down_filter)
        return self

    def build(self) -> Dict[str, Any]:
        if self._push_down_filters:
            self._config['streams'] = self._push_down_filters

        return self._config
