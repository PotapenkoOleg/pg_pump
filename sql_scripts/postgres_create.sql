CREATE DATABASE developer;

CREATE SCHEMA "Sample";

CREATE TABLE "Sample"."TestData1"
(
    "ID"         BIGINT,
    "FileNumber" INT,
    "Code"       VARCHAR(100)
);

--DROP TABLE "Sample"."TestData1";

TRUNCATE TABLE "Sample"."TestData1";

SELECT * FROM "Sample"."TestData1";

SELECT COUNT(*) FROM "Sample"."TestData1";

SELECT COUNT(*) FROM "Sample"."TestData1" GROUP BY "FileNumber";

DROP TABLE "Sample"."AllTypes";

CREATE TABLE "Sample"."AllTypes"
(
    "ID"         BIGINT,
    "T_BIT"      BOOLEAN,
    "T_BYTE"     SMALLINT,
    "T_INT"      INT,
    "T_LONG"     BIGINT,
    "T_DATE"     DATE,
    "T_TIME"     TIME,
    "T_DATETIME" TIMESTAMP,
    "T_DECIMAL"  DECIMAL(18, 2),
    "T_FLOAT"    FLOAT(53),
    "T_GUID"     UUID,
    "T_CHAR"     CHAR(1),
    "T_NVARCHAR" VARCHAR(255),
    "T_VARCHAR"  VARCHAR(255)
);

SELECT * FROM "Sample"."AllTypes";
