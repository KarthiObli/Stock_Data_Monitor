CREATE OR REPLACE PROCEDURE "Consumable_Layer"."Stored_Procedure_Load_Tech_Mahindra_Processed_to_Monthly_Data"()
LANGUAGE 'plpgsql'
AS 
$$
BEGIN
TRUNCATE TABLE "Consumable_Layer"."Tech_Mahindra_Monthly_Data";
INSERT INTO "Consumable_Layer"."Tech_Mahindra_Monthly_Data"
(
SELECT "Date",
	CONCAT(
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
		 	RIGHT((CAST((EXTRACT(YEAR from "Date")) AS varchar)),2)), 
			"Price" FROM "Processed_Layer"."Tech_Mahindra_Processed_Data"  WHERE "Date" in 
				(SELECT "Date" FROM 
					(
					SELECT "Month_Id", MAX("Date") as "Date" from "Processed_Layer"."Tech_Mahindra_Processed_Data"  GROUP BY 1
					)
				)
			ORDER BY "Month_Id"			
	);
END;
$$;
