from datetime import datetime
from typing import Any, Dict


class ConfigBuilder:
    def __init__(self, jwt_token: str, host:str, schema:str, database:str, role:str,warehouse:str) -> None:
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
                "method":"standard"
            }
        }
    

    def build(self) -> Dict[str, Any]:
        return self._config
    

