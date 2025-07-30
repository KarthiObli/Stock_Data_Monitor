CREATE OR REPLACE PROCEDURE "Consumable_Layer"."Stored_Procedure_Load_TCS_Processed_to_Daily_Data_L3M"()
language plpgsql
AS 
$$
BEGIN
TRUNCATE TABLE "Consumable_Layer"."TCS_Daily_Data_L3M" ;
INSERT INTO "Consumable_Layer"."TCS_Daily_Data_L3M" 
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
		"Price" FROM "Processed_Layer"."TCS_Processed_Data"  WHERE "Month_Id" IN (SELECT DISTINCT "Month_Id" FROM "Processed_Layer"."TCS_Processed_Data"   ORDER BY 1 DESC LIMIT 3)
		ORDER BY "Date";
END;
END;
$$;	
