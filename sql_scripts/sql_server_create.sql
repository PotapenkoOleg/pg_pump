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