from datetime import datetime
from typing import Any, Dict


class ConfigBuilder:
    def __init__(self) -> None:
        self._config: Dict[str, Any] = {
            "jwt_token": "ConfigBuilder default client secret",
            "account_identifier": "ConfigBuilder default account id",
        }

    def with_account_id(self, account_identifier: str) -> "ConfigBuilder":
        self._config["account_identifier"] = account_identifier
        return self

    def with_jwt_token(self, jwt_token: str) -> "ConfigBuilder":
        self._config["jwt_token"] = jwt_token
        return self

    def with_cursor_field(self, cursor_field: str) -> "ConfigBuilder":
        self._config["cursor_field"] = cursor_field
        return self

    def build(self) -> Dict[str, Any]:
        return self._config
