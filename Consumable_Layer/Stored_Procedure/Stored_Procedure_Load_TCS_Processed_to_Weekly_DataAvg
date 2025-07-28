CREATE OR REPLACE PROCEDURE "Consumable_Layer"."Stored_Procedure_Load_TCS_Processed_to_Weekly_DataAvg"()
language plpgsql
AS 
$$
BEGIN
TRUNCATE TABLE "Consumable_Layer"."TCS_Weekly_Data";
INSERT INTO "Consumable_Layer"."TCS_Weekly_Data"
(
WITH
a as
	(SELECT DATE_TRUNC ('week', "Date") as week_start,"Date", "Price" FROM "Processed_Layer"."TCS_Processed_Data" ),	
b as 
	(SELECT "Date",
	
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
	"Price" FROM a WHERE "Date" IN (SELECT "Date" FROM
	(SELECT week_start, MAX("Date") as "Date" FROM a
	GROUP BY 1))
	ORDER BY "Date")
select * from b
);
END;
$$;	
