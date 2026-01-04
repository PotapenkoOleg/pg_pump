CREATE DATABASE developer;

USE developer;

CREATE SCHEMA [Sample];

CREATE TABLE [Sample].[TestData1]
(
    ID         BIGINT,
    FileNumber INT,
    Code       VARCHAR(100)
    );
--DROP TABLE [Sample].[TestData1]

SELECT count(*) FROM [Sample].[TestData1];

CREATE TABLE [Sample].[AllTypes]
(
    ID         BIGINT,
    T_BIT      BIT,
    T_BYTE     SMALLINT,
    T_INT      INT,
    T_LONG     BIGINT,
    T_DATE     DATE,
    T_TIME     TIME,
    T_DATETIME DATETIME2,
    T_DECIMAL  DECIMAL(18, 2),
    T_FLOAT    FLOAT(53),
    T_GUID     UNIQUEIDENTIFIER,
    T_CHAR     CHAR(1),
    T_NVARCHAR NVARCHAR(255),
    T_VARCHAR  VARCHAR(255)
    );


INSERT INTO [Sample].[AllTypes] (ID,
                                 T_BIT,
                                 T_BYTE,
                                 T_INT,
                                 T_LONG,
                                 T_DATE,
                                 T_TIME,
                                 T_DATETIME,
                                 T_DECIMAL,
                                 T_FLOAT,
                                 T_GUID,
                                 T_CHAR,
                                 T_NVARCHAR,
                                 T_VARCHAR)
VALUES (
    1,
    1,
    1,
    1,
    1,
    '2020-01-01',
    '12:00:00',
    '2020-01-01 12:00:00',
    1.00,
    1.00,
    '00000000-0000-0000-0000-000000000000',
    'A',
    'AAA',
    'AAA');

INSERT INTO [Sample].[AllTypes] (ID,
                                 T_BIT,
                                 T_BYTE,
                                 T_INT,
                                 T_LONG,
                                 T_DATE,
                                 T_TIME,
                                 T_DATETIME,
                                 T_DECIMAL,
                                 T_FLOAT,
                                 T_GUID,
                                 T_CHAR,
                                 T_NVARCHAR,
                                 T_VARCHAR)
VALUES (
    1,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL);

SELECT * FROM [Sample].[AllTypes];

----------
DROP TABLE [Sample].[AllTypes];

CREATE TABLE [Sample].[AllTypes]
(
    ID         BIGINT,
    T_BIT      BIT,
    T_BYTE     SMALLINT,
    T_INT      INT,
    T_LONG     BIGINT,
    T_DATE     DATE,
    T_TIME     TIME,
    T_DATETIME DATETIME2,
    T_DECIMAL  DECIMAL(18, 2),
    T_FLOAT    FLOAT(53),
    T_GUID     UNIQUEIDENTIFIER,
    T_CHAR     CHAR(1),
    T_NCHAR    NCHAR(1),
    T_VARCHAR  VARCHAR(255),
    T_NVARCHAR NVARCHAR(255)

    );

INSERT INTO [Sample].[AllTypes] (ID,
                                 T_BIT,
                                 T_BYTE,
                                 T_INT,
                                 T_LONG,
                                 T_DATE,
                                 T_TIME,
                                 T_DATETIME,
                                 T_DECIMAL,
                                 T_FLOAT,
                                 T_GUID,
                                 T_CHAR,
                                 T_NCHAR,
                                 T_VARCHAR,
                                 T_NVARCHAR

)
VALUES (
    1,
    1,
    1,
    1,
    1,
    '2020-01-01',
    '12:00:00',
    '2020-01-01 12:00:00',
    1.00,
    1.13,
    '00000000-0000-0000-0000-000000000000',
    'A',
    'A',
    'AAA',
    'AAA'
    );

SELECT * FROM [Sample].[AllTypes];