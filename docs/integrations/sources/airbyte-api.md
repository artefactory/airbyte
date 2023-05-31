# Airbyte API

## Setup
To setup this source an API key is required. To get an API key, login in the airbyte cloud [developper portal](https://portal.airbyte.com/apiKeys) and create one with read rigts .


## Features

| Feature                   | Supported? |
| :------------------------ | :--------- |
| Full Refresh Sync         | Yes        |
| Incremental - Append Sync | Yes        |
| SSL connection            | No         |
| Namespaces                | No         |

## Supporrted streams

| Stream name | API Documentation | Mode |
| :---------- | :---------------- | :--- |
| Workspaces  | [API Documentation](https://docs.airbyte.io/api/sources/workspaces) | Full Refresh |
| Connections | [API Documentation](https://docs.airbyte.io/api/sources/connections) | Full Refresh |
| Sources     | [API Documentation](https://docs.airbyte.io/api/sources/sources) | Full Refresh |
| Jobs        | [API Documentation](https://docs.airbyte.io/api/sources/jobs) | Full Refresh\incremental |



