# Snowflake

## Overview

The Snowflake source allows you to sync data from Snowflake. It supports both Full Refresh and Incremental syncs. You can choose if this connector will copy only the new or updated data, or all rows in the tables and columns you set up for replication, every time a sync is run.

This Snowflake source connector is built on top of the REST API V2 of Snowflake as described in Snowflake [documentation](https://docs.snowflake.com/en/developer-guide/sql-api/index)

#### Resulting schema

The Snowflake source does not alter the schema present in your warehouse. Depending on the destination connected to this source, however, the result schema may be altered. See the destination's documentation for more details.

#### Features

| Feature                   | Supported?\(Yes/No\) | Notes |
| :------------------------ | :------------------- | :---- |
| Full Refresh Sync         | Yes                  |       |
| Incremental - Append Sync | Yes                  |       |
| Namespaces                | Yes                  |       |

## Getting started

### Requirements

1. You'll need the following information to configure the Snowflake source:
2. **Host (account name)**
3. **Role**
4. **Warehouse**
5. **Database**
6. **Schema**
7. **Username**
8. **Private key**
9. **Password** 
10. Create a dedicated read-only Airbyte user and role with access to all schemas needed for replication and add key-pair authentication 
### Setup guide

#### 1. Create a dedicated read-only user with access to the relevant schemas \(Recommended but optional\)

This step is optional but highly recommended to allow for better permission control and auditing. Alternatively, you can use Airbyte with an existing user in your database.

Create a dedicated database user.

You can limit this grant down to specific schemas instead of the whole database. Note that to replicate data from multiple Snowflake databases, you can re-run the command above to grant access to all the relevant schemas, but you'll need to set up multiple sources connecting to the same db on multiple schemas.

Your database user should now be ready for use with Airbyte.

### Authentication

#### Login and Password
#### 2.Create key pair for authentication 
You have to follow these steps to create your public and private key to authenticate to the application:

##### A. create a private key
You have 2 choices for private key creation, either with password or without password

No password will be asked since you are using the the nocrypt argument. Use this command:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

If you want to set a password, run this command:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

Note: You will need this password later if you want to authenticate. So store it securely.

The commands generate a private key in PEM format. A file named rsa_key.p8 will be generated in your current working directory.

-----BEGIN ENCRYPTED PRIVATE KEY-----   
MIIE6T...   
-----END ENCRYPTED PRIVATE KEY----   

##### B. create a public key

Run this command in the same directory as your private key:

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Ii will generate a file rsa_key.pub containing your public key.
The command generates the public key in PEM format.

-----BEGIN PUBLIC KEY-----  
MIIBIj...  
-----END PUBLIC KEY-----  

##### C. Assign the public key to a user
Store the content of your public key with this SQL statement.

```sql
ALTER USER jsmith SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```
Note:  
Only owners of a user, or users with the SECURITYADMIN role or higher can alter a user.  
Exclude the public key delimiters in the SQL statement.  


##### D. Verify the upload of the key 
To achieve that, we will compare the public key fingerprint. 

Generate the public key fingerprint in snowflake with this command

```sql
ALTER USER jsmith SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```
Output:  
Azk1Pq...

Generate the public key fingerprint in your terminal with this command

```bash
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```
Output:  
Azk1Pq...

Make sure they are the same, if not repeat step C.

That was the last step if you want more details, look at the following link [here](https://docs.snowflake.com/en/user-guide/key-pair-auth).


###Authentication

#### There are 2 way ways of oauth supported: login\pass and oauth2.

### JWT Token

| Field                                                                                                 | Description                                                                                                                                                                                       |
|-------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Host](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html)                        | The host domain of the snowflake instance (must include the account, region, cloud environment, and end with snowflakecomputing.com). Example: `accountname.us-east-2.aws.snowflakecomputing.com` |
| [Role](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#roles)          | The role you created in Step 1 for Airbyte to access Snowflake. Example: `AIRBYTE_ROLE`                                                                                                           |
| [Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#overview-of-warehouses) | The warehouse you created in Step 1 for Airbyte to sync data into. Example: `AIRBYTE_WAREHOUSE`                                                                                                   |
| [Database](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)   | The database you created in Step 1 for Airbyte to sync data into. Example: `AIRBYTE_DATABASE`                                                                                                     |
| [Schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)     | The schema whose tables this replication is targeting. If no schema is specified, all tables with permission will be presented regardless of their schema.                                        |
| Username                                                                                              | The username you created in Step 2 to allow Airbyte to access the database. Example: `AIRBYTE_USER`                                                                                               |
| Private key                                                                                           | The secret key used when creating the key pair authentication                                                                                                                                     |
| Password                                                                                              | The password of you secret key (optional)                                                                                                                                                         |

### OAuth 2.0 TO BE UPDATED

| Field                                                                                                 | Description                                                                                                                                                                                       |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Host](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html)                        | The host domain of the snowflake instance (must include the account, region, cloud environment, and end with snowflakecomputing.com). Example: `accountname.us-east-2.aws.snowflakecomputing.com` |
| [Role](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#roles)          | The role you created in Step 1 for Airbyte to access Snowflake. Example: `AIRBYTE_ROLE`                                                                                                           |
| [Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#overview-of-warehouses) | The warehouse you created in Step 1 for Airbyte to sync data into. Example: `AIRBYTE_WAREHOUSE`                                                                                                   |
| [Database](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)   | The database you created in Step 1 for Airbyte to sync data into. Example: `AIRBYTE_DATABASE`                                                                                                     |
| [Schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)     | The schema whose tables this replication is targeting. If no schema is specified, all tables with permission will be presented regardless of their schema.                                        |
| OAuth2                                                                                                | The Login name and password to obtain auth token.                                                                                                                                                 |
### Network policies

By default, Snowflake allows users to connect to the service from any computer or device IP address. A security administrator (i.e. users with the SECURITYADMIN role) or higher can create a network policy to allow or deny access to a single IP address or a list of addresses.

If you have any issues connecting with Airbyte Cloud please make sure that the list of IP addresses is on the allowed list

To determine whether a network policy is set on your account or for a specific user, execute the _SHOW PARAMETERS_ command.

**Account**

```
SHOW PARAMETERS LIKE 'network_policy' IN ACCOUNT;
```

**User**

```
SHOW PARAMETERS LIKE 'network_policy' IN USER <username>;
```

To read more please check official [Snowflake documentation](https://docs.snowflake.com/en/user-guide/network-policies.html#)  

## Note
You can find details about allowed retention days here: https://docs.snowflake.com/en/user-guide/data-time-travel  


## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                                                                                                                   |
|:--------|:-----------|:---------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| 0.3.3   | 2024-05-30 | Fill with right pull request                             | Implement connector in python  
| :------ | :--------- | :------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------- |
| 0.3.2   | 2024-02-13 | [38317](https://github.com/airbytehq/airbyte/pull/38317) | Hide oAuth option from connector                                                                                                          |
| 0.3.1   | 2024-02-13 | [35220](https://github.com/airbytehq/airbyte/pull/35220) | Adopt CDK 0.20.4                                                                                                                          |
| 0.3.1   | 2024-01-24 | [34453](https://github.com/airbytehq/airbyte/pull/34453) | bump CDK version                                                                                                                          |
| 0.3.0   | 2023-12-18 | [33484](https://github.com/airbytehq/airbyte/pull/33484) | Remove LEGACY state                                                                                                                       |
| 0.2.2   | 2023-10-20 | [31613](https://github.com/airbytehq/airbyte/pull/31613) | Fixed handling of TIMESTAMP_TZ columns. upgrade                                                                                           |
| 0.2.1   | 2023-10-11 | [31252](https://github.com/airbytehq/airbyte/pull/31252) | Snowflake JDBC version upgrade                                                                                                            |
| 0.2.0   | 2023-06-26 | [27737](https://github.com/airbytehq/airbyte/pull/27737) | License Update: Elv2                                                                                                                      |
| 0.1.36  | 2023-06-20 | [27212](https://github.com/airbytehq/airbyte/pull/27212) | Fix silent exception swallowing in StreamingJdbcDatabase                                                                                  |
| 0.1.35  | 2023-06-14 | [27335](https://github.com/airbytehq/airbyte/pull/27335) | Remove noisy debug logs                                                                                                                   |
| 0.1.34  | 2023-03-30 | [24693](https://github.com/airbytehq/airbyte/pull/24693) | Fix failure with TIMESTAMP_WITH_TIMEZONE column being used as cursor                                                                      |
| 0.1.33  | 2023-03-29 | [24667](https://github.com/airbytehq/airbyte/pull/24667) | Fix bug which wont allow TIMESTAMP_WITH_TIMEZONE column to be used as a cursor                                                            |
| 0.1.32  | 2023-03-22 | [20760](https://github.com/airbytehq/airbyte/pull/20760) | Removed redundant date-time datatypes formatting                                                                                          |
| 0.1.31  | 2023-03-06 | [23455](https://github.com/airbytehq/airbyte/pull/23455) | For network isolation, source connector accepts a list of hosts it is allowed to connect to                                               |
| 0.1.30  | 2023-02-21 | [22358](https://github.com/airbytehq/airbyte/pull/22358) | Improved handling of big integer cursor type values.                                                                                      |
| 0.1.29  | 2022-12-14 | [20436](https://github.com/airbytehq/airbyte/pull/20346) | Consolidate date/time values mapping for JDBC sources.                                                                                    |
| 0.1.28  | 2023-01-06 | [20465](https://github.com/airbytehq/airbyte/pull/20465) | Improve the schema config field to only discover tables from the specified scehma and make the field optional                             |
| 0.1.27  | 2022-12-14 | [20407](https://github.com/airbytehq/airbyte/pull/20407) | Fix an issue with integer values converted to floats during replication                                                                   |
| 0.1.26  | 2022-11-10 | [19314](https://github.com/airbytehq/airbyte/pull/19314) | Set application id in JDBC URL params based on OSS/Cloud environment                                                                      |
| 0.1.25  | 2022-11-10 | [15535](https://github.com/airbytehq/airbyte/pull/15535) | Update incremental query to avoid data missing when new data is inserted at the same time as a sync starts under non-CDC incremental mode |
| 0.1.24  | 2022-09-26 | [17144](https://github.com/airbytehq/airbyte/pull/17144) | Fixed bug with incorrect date-time datatypes handling                                                                                     |
| 0.1.23  | 2022-09-26 | [17116](https://github.com/airbytehq/airbyte/pull/17116) | added connection string identifier                                                                                                        |
| 0.1.22  | 2022-09-21 | [16766](https://github.com/airbytehq/airbyte/pull/16766) | Update JDBC Driver version to 3.13.22                                                                                                     |
| 0.1.21  | 2022-09-14 | [15668](https://github.com/airbytehq/airbyte/pull/15668) | Wrap logs in AirbyteLogMessage                                                                                                            |
| 0.1.20  | 2022-09-01 | [16258](https://github.com/airbytehq/airbyte/pull/16258) | Emit state messages more frequently                                                                                                       |
| 0.1.19  | 2022-08-19 | [15797](https://github.com/airbytehq/airbyte/pull/15797) | Allow using role during oauth                                                                                                             |
| 0.1.18  | 2022-08-18 | [14356](https://github.com/airbytehq/airbyte/pull/14356) | DB Sources: only show a table can sync incrementally if at least one column can be used as a cursor field                                 |
| 0.1.17  | 2022-08-09 | [15314](https://github.com/airbytehq/airbyte/pull/15314) | Discover integer columns as integers rather than floats                                                                                   |
| 0.1.16  | 2022-08-04 | [15314](https://github.com/airbytehq/airbyte/pull/15314) | (broken, do not use) Discover integer columns as integers rather than floats                                                              |
| 0.1.15  | 2022-07-22 | [14828](https://github.com/airbytehq/airbyte/pull/14828) | Source Snowflake: Source/Destination doesn't respect DATE data type                                                                       |
| 0.1.14  | 2022-07-22 | [14714](https://github.com/airbytehq/airbyte/pull/14714) | Clarified error message when invalid cursor column selected                                                                               |
| 0.1.13  | 2022-07-14 | [14574](https://github.com/airbytehq/airbyte/pull/14574) | Removed additionalProperties:false from JDBC source connectors                                                                            |
| 0.1.12  | 2022-04-29 | [12480](https://github.com/airbytehq/airbyte/pull/12480) | Query tables with adaptive fetch size to optimize JDBC memory consumption                                                                 |
| 0.1.11  | 2022-04-27 | [10953](https://github.com/airbytehq/airbyte/pull/10953) | Implement OAuth flow                                                                                                                      |
| 0.1.9   | 2022-02-21 | [10242](https://github.com/airbytehq/airbyte/pull/10242) | Fixed cursor for old connectors that use non-microsecond format. Now connectors work with both formats                                    |
| 0.1.8   | 2022-02-18 | [10242](https://github.com/airbytehq/airbyte/pull/10242) | Updated timestamp transformation with microseconds                                                                                        |
| 0.1.7   | 2022-02-14 | [10256](https://github.com/airbytehq/airbyte/pull/10256) | Add `-XX:+ExitOnOutOfMemoryError` JVM option                                                                                              |
| 0.1.6   | 2022-01-25 | [9623](https://github.com/airbytehq/airbyte/pull/9623)   | Add jdbc_url_params support for optional JDBC parameters                                                                                  |
| 0.1.5   | 2022-01-19 | [9567](https://github.com/airbytehq/airbyte/pull/9567)   | Added parameter for keeping JDBC session alive                                                                                            |
| 0.1.4   | 2021-12-30 | [9203](https://github.com/airbytehq/airbyte/pull/9203)   | Update connector fields title/description                                                                                                 |
| 0.1.3   | 2021-01-11 | [9304](https://github.com/airbytehq/airbyte/pull/9304)   | Upgrade version of JDBC driver                                                                                                            |
| 0.1.2   | 2021-10-21 | [7257](https://github.com/airbytehq/airbyte/pull/7257)   | Fixed parsing of extreme values for FLOAT and NUMBER data types                                                                           |
| 0.1.1   | 2021-08-13 | [4699](https://github.com/airbytehq/airbyte/pull/4699)   | Added json config validator                                                                                                               |

</details>