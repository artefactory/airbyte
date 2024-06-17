from source_snowflake import SnowflakeJwtAuthenticator

def test_get_formatted_password_from_config():
    config = {}
    SnowflakeJwtAuthenticator.get_formatted_password_from_config(config)


def test_get_formatted_account():
    account, user_name = '', ''
    SnowflakeJwtAuthenticator.get_formatted_account(account, user_name)


def test_format_private_key():
    private_key = ''
    SnowflakeJwtAuthenticator._format_private_key(private_key)
