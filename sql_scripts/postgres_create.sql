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