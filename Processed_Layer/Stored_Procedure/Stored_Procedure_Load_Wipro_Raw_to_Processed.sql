CREATE OR REPLACE PROCEDURE "Processed_Layer"."Stored_Procedure_Load_Wipro_Raw_to_Processed"()
language plpgsql
AS
$$
DECLARE month_no integer; 
BEGIN
IF EXISTS (SELECT 1 FROM "Raw_Layer"."Wipro_Raw_Data")
THEN
CREATE TEMP TABLE common_month_label AS SELECT * FROM (
WITH month_display AS
(
	SELECT DISTINCT CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) AS month_label FROM "Raw_Layer"."Wipro_Raw_Data"
),
common AS 
(
	SELECT month_label FROM month_display WHERE month_label IN (SELECT DISTINCT CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) FROM "Processed_Layer"."Wipro_Processed_Data")
)
SELECT *,0 as data_change FROM common);

IF EXISTS (SELECT 1 FROM common_month_label)
THEN
UPDate common_month_label SET data_change = 1 WHERE month_label IN 
(SELECT DISTINCT CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) FROM
(SELECT "Date","Price" FROM "Raw_Layer"."Wipro_Raw_Data" WHERE CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) IN (SELECT month_label FROM common_month_label)
EXCEPT DISTINCT
SELECT "Date","Price" FROM "Processed_Layer"."Wipro_Processed_Data" WHERE CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) IN (SELECT month_label FROM common_month_label)));

CREATE TEMP TABLE temp_process AS (SELECT * FROM "Processed_Layer"."Wipro_Processed_Data");

DELETE FROM temp_process WHERE CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) IN (SELECT month_label FROM common_month_label WHERE data_change = 1);

INSERT INTO temp_process ("Date","Price") 
SELECT "Date","Price" FROM "Raw_Layer"."Wipro_Raw_Data" WHERE CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) IN (SELECT month_label FROM common_month_label WHERE data_change = 1);

DELETE FROM "Raw_Layer"."Wipro_Raw_Data" WHERE CONCAT(EXTRACT(MONTH FROM "Date"),'-', EXTRACT(YEAR FROM "Date")) IN (SELECT month_label FROM common_month_label);


TRUNCATE "Processed_Layer"."Wipro_Processed_Data";


INSERT INTO "Processed_Layer"."Wipro_Processed_Data" ("Month_Id","Date","Price")
(WITH a AS
(
	SELECT "Date", "Price", DATE_TRUNC('month',"Date") AS month_start
	FROM temp_process
	GROUP BY 1,2
	ORDER BY 3 
)
,
b AS 
(
	SELECT DENSE_RANK() OVER (ORDER BY month_start) AS "Month_Id","Date","Price" from a
)
SELECT * FROM b ORDER BY "Month_Id");

DROP TABLE temp_process;

DROP TABLE common_month_label;
ELSE 
DROP TABLE common_month_label;
END IF;

month_no := (SELECT COUNT(DISTINCT "Month_Id")from "Processed_Layer"."Wipro_Processed_Data");
IF (month_no = 12)
THEN
	DELETE from "Processed_Layer"."Wipro_Processed_Data" WHERE "Month_Id" IN 
(SELECT DISTINCT "Month_Id" FROM "Processed_Layer"."Wipro_Processed_Data" ORDER BY 1 LIMIT (SELECT COUNT(DISTINCT EXTRACT(month from "Date")) from "Raw_Layer"."Wipro_Raw_Data"));
END IF;

CREATE TEMP TABLE process_dup AS (SELECT * FROM "Processed_Layer"."Wipro_Processed_Data" LIMIT 0);
INSERT INTO process_dup
(
	WITH a AS (
		SELECT "Date","Price" FROM "Processed_Layer"."Wipro_Processed_Data"
		UNION ALL
		SELECT "Date","Price" FROM "Raw_Layer"."Wipro_Raw_Data"
	),
	b AS (
		SELECT DATE_TRUNC('month',"Date") as month_start,"Date","Price" FROM a
	),
	c AS (
		SELECT DENSE_RANK() OVER (ORDER BY month_start) AS "Month_Id","Date","Price" from b
	)
	SELECT * FROM c
);
TRUNCATE TABLE "Processed_Layer"."Wipro_Processed_Data";
INSERT INTO "Processed_Layer"."Wipro_Processed_Data"
(
	SELECT * FROM process_dup
);
DROP TABLE process_dup;
END IF;
IF (SELECT COUNT(DISTINCT "Month_Id") FROM "Processed_Layer"."Wipro_Processed_Data") > 12
THEN
CREATE TEMP TABLE process_dup_1 AS (SELECT * FROM "Processed_Layer"."Wipro_Processed_Data" LIMIT 0);
DELETE FROM "Processed_Layer"."Wipro_Processed_Data" WHERE "Month_Id" IN (SELECT DISTINCT "Month_Id" FROM "Processed_Layer"."Wipro_Processed_Data" ORDER BY 1 LIMIT ( SELECT ((SELECT COUNT(DISTINCT "Month_Id") FROM "Processed_Layer"."Wipro_Processed_Data") - 12)));  
INSERT INTO process_dup_1
(
WITH a AS (
		SELECT "Date","Price" FROM "Processed_Layer"."Wipro_Processed_Data"
	),
	b AS (
		SELECT DATE_TRUNC('month',"Date") as month_start,"Date","Price" FROM a
	),
	c AS (
		SELECT DENSE_RANK() OVER (ORDER BY month_start) AS "Month_Id","Date","Price" from b
	)
	SELECT * FROM c
);
TRUNCATE TABLE "Processed_Layer"."Wipro_Processed_Data";
INSERT INTO "Processed_Layer"."Wipro_Processed_Data"
(
	SELECT * FROM process_dup_1
);
DROP TABLE process_dup_1;
END IF;
END;
$$;
