CREATE SCHEMA INTEGRATION_TEST;

CREATE OR REPLACE TABLE
        INTEGRATION_TEST.TEST_TABLE(
            ID INTEGER PRIMARY KEY,
            TEST_COLUMN_1 NUMBER,
            TEST_COLUMN_10 NUMBER(
                10,
                5
            ),
            TEST_COLUMN_11 DOUBLE,
            TEST_COLUMN_12 FLOAT,
            TEST_COLUMN_14 VARCHAR,
            TEST_COLUMN_15 STRING,
            TEST_COLUMN_16 TEXT,
            TEST_COLUMN_17 CHAR,
            TEST_COLUMN_18 BINARY,
            TEST_COLUMN_19 BOOLEAN,
            TEST_COLUMN_2 DECIMAL,
            TEST_COLUMN_20 DATE,
            TEST_COLUMN_21 DATETIME,
            TEST_COLUMN_22 TIME,
            TEST_COLUMN_23 TIMESTAMP,
            TEST_COLUMN_24 TIMESTAMP_LTZ,
            TEST_COLUMN_25 TIMESTAMP_NTZ,
            TEST_COLUMN_26 TIMESTAMP_TZ,
            TEST_COLUMN_27 VARIANT,
            TEST_COLUMN_28 ARRAY,
            TEST_COLUMN_29 OBJECT,
            TEST_COLUMN_3 NUMERIC,
            TEST_COLUMN_30 GEOGRAPHY,
            TEST_COLUMN_4 BIGINT,
            TEST_COLUMN_5 INT,
            TEST_COLUMN_6 BIGINT,
            TEST_COLUMN_7 SMALLINT,
            TEST_COLUMN_8 TINYINT,
            TEST_COLUMN_9 BYTEINT
        );

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            1,
            99999999999999999999999999999999999999,
            10.12345,
            - 9007199254740991,
            10e - 308,
            'тест',
            'テスト',
            '-!-',
            'a',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            'true',
            9223372036854775807,
            '0001-01-01',
            '0001-01-01 00:00:00',
            '00:00:00',
            '2018-03-22 12:00:00.123',
            '2018-03-22 12:00:00.123 +05:00',
            '2018-03-22 12:00:00.123 +05:00',
            '2018-03-22 12:00:00.123 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            99999999999999999999999999999999999999,
            'POINT(-122.35 37.55)',
            99999999999999999999999999999999999999,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807,
            9223372036854775807;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            2,
            - 99999999999999999999999999999999999999,
            10.12345,
            9007199254740991,
            10e + 307,
            '⚡ test ��',
            'テスト',
            '-\x25-',
            'ス',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            5,
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59',
            '1:59 PM',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 99999999999999999999999999999999999999,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            3,
            9223372036854775807,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            '!',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            'false',
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            9223372036854775807,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            4,
            - 9223372036854775808,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            'ї',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            0,
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 9223372036854775808,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            5,
            - 9223372036854775808,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            'ї',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            TO_BOOLEAN('y'),
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 9223372036854775808,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            6,
            - 9223372036854775808,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            'ї',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            TO_BOOLEAN('n'),
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 9223372036854775808,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;

INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            7,
            - 9223372036854775808,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            'ї',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            TO_BOOLEAN('n'),
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 9223372036854775808,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;


INSERT
    INTO
        INTEGRATION_TEST.TEST_TABLE SELECT
            8,
            - 9223372036854775808,
            10.12345,
            9007199254740991,
            10e + 307,
            '!"#$%&''()*+,-./:;<=>?\@[\]^_\`{|}~',
            'テスト',
            '-\x25-',
            'ї',
            to_binary(
                'HELP',
                'UTF-8'
            ),
            TO_BOOLEAN('n'),
            - 9223372036854775808,
            '9999-12-31',
            '9999-12-31 23:59:59.123456',
            '23:59:59.123456',
            '2018-03-22 12:00:00.123456',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            '2018-03-22 12:00:00.123456 +05:00',
            parse_json(' { "key1": "value1", "key2": "value2" } '),
            array_construct(
                1,
                2,
                3
            ),
            parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, "outer_key2": { "inner_key2": 2 } } '),
            - 9223372036854775808,
            'LINESTRING(-124.20 42.00, -120.01 41.99)',
            - 99999999999999999999999999999999999999,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808,
            - 9223372036854775808;
-- Enable change tracking on the table.
ALTER TABLE INTEGRATION_TEST.TEST_TABLE SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE INTEGRATION_TEST.TEST_TABLE SET CHANGE_TRACKING = TRUE;


-- CHANGES to populate changes UPDATE AND DELETE
DELETE FROM INTEGRATION_TEST.TEST_TABLE WHERE TEST_COLUMN_1 = 8
UPDATE INTEGRATION_TEST.TEST_TABLE SET TEST_COLUMN_1 = 9 WHERE TEST_COLUMN_1 = 7

