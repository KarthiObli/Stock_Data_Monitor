CREATE OR REPLACE PROCEDURE "Consumable_Layer"."Stored_Procedure_Load_Tech_Mahindra_Processed_to_Daily_Data_QTD"()
language plpgsql
AS 
$$
BEGIN
TRUNCATE TABLE "Consumable_Layer"."Tech_Mahindra_Daily_Data_QTD";
INSERT INTO "Consumable_Layer"."Tech_Mahindra_Daily_Data_QTD"
(
WITH a as 
(
	SELECT CASE 
		WHEN EXTRACT (MONTH from "Date") IN (4,5,6) THEN CONCAT ('Q1',' ',RIGHT((CAST(((EXTRACT (YEAR FROM "Date"))+1) AS varchar)),2))
		WHEN EXTRACT (MONTH from "Date") IN (7,8,9) THEN CONCAT ('Q2',' ',RIGHT((CAST(((EXTRACT (YEAR FROM "Date"))+1) AS varchar)),2))
		WHEN EXTRACT (MONTH from "Date") IN (10,11,12) THEN CONCAT ('Q3',' ',RIGHT((CAST(((EXTRACT (YEAR FROM "Date"))+1) AS varchar)),2))
		WHEN EXTRACT (MONTH from "Date") IN (1,2,3) THEN CONCAT ('Q4',' ',RIGHT((CAST(((EXTRACT (YEAR FROM "Date"))) AS varchar)),2))
		END as quarter, 
		"Date", 
		"Price" 
		from "Processed_Layer"."Tech_Mahindra_Processed_Data"
		ORDER BY "Date"),
b as
	(
			SELECT "Date",quarter FROM (SELECT DISTINCT quarter,"Date" FROM a WHERE "Date" IN (SELECT "Date" FROM (SELECT quarter, max("Date") FROM a GROUP BY 1 )) ORDER BY "Date" DESC LIMIT 1) 
	),
c as
	(
		SELECT "Date",
				CONCAT (
	EXTRACT (day from "Date"),
	' ',
	CASE 
			WHEN (EXTRACT(month from "Date")=1) THEN 'Jan'
			WHEN (EXTRACT(month from "Date")=2) THEN 'Feb'
			WHEN (EXTRACT(month from "Date")=3) THEN 'Mar'
			WHEN (EXTRACT(month from "Date")=4) THEN 'Apr'
			WHEN (EXTRACT(month from "Date")=5) THEN 'May'
			WHEN (EXTRACT(month from "Date")=6) THEN 'Jun'
			WHEN (EXTRACT(month from "Date")=7) THEN 'Jul'
			WHEN (EXTRACT(month from "Date")=8) THEN 'Aug'
			WHEN (EXTRACT(month from "Date")=9) THEN 'Sep'
			WHEN (EXTRACT(month from "Date")=10) THEN 'Oct'
			WHEN (EXTRACT(month from "Date")=11) THEN 'Nov'
			WHEN (EXTRACT(month from "Date")=12) THEN 'Dec'
			END,
		' ',
		RIGHT((CAST((EXTRACT (year from "Date")) AS varchar)),2)),
		"Price" FROM a WHERE quarter = (SELECT quarter FROM b)
	)
SELECT * FROM c );
END;
$$;
