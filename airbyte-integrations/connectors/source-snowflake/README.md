# Snowflake Source

## Documentation

- [User Documentation](https://docs.airbyte.io/integrations/sources/snowflake)

## Community Contributor

1. Look at the integration documentation to see how to create a warehouse/database/schema/user/role for Airbyte to sync into.
1. Create a file at `secrets/config.json` with the following format:

```


### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/snowflake)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `src/source_snowflake/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.


### Locally running the connector

```

3. Create a file at `secrets/config_auth.json` with the following format:

```

### Running tests

To run tests locally, from the connector directory run:

```

## For Airbyte employees

To be able to run integration tests locally:

1. Put the contents of the `Source snowflake test creds (secrets/config.json)` secret on Lastpass into `secrets/config.json`.
1. Put the contents of the `SECRET_SOURCE-SNOWFLAKE_OAUTH__CREDS (secrets/config_auth.json)` secret on Lastpass into `secrets/config_auth.json`.
