from typing import Any, Dict

from airbyte_cdk.test.mock_http.response_builder import PaginationStrategy


class SnowflakePaginationStrategy(PaginationStrategy):
    @staticmethod
    def update(response: Dict[str, Any]) -> None:
        response["resultSetMetaData"]["partitionInfo"] = [
            {
                "rowCount": 12344,
                "uncompressedSize": 14384873,
            }, {
                "rowCount": 47387,
                "uncompressedSize": 76483423,
                "compressedSize": 4342748
            }, {
                "rowCount": 43746,
                "uncompressedSize": 43748274,
                "compressedSize": 746323
            }]
