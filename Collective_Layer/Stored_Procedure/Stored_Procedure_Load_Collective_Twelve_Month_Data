CREATE OR REPLACE PROCEDURE "Collective_Layer"."Stored_Procedure_Load_Collective_Twelve_Month_Data"()
language plpgsql
AS
$$
BEGIN
TRUNCATE TABLE "Collective_Layer"."Collective_Twelve_Month_Data";
IF ((SELECT MAX("Month_Id") FROM "Processed_Layer"."HCL_Processed_Data") = 12) THEN
INSERT INTO "Collective_Layer"."Collective_Twelve_Month_Data"
WITH max_price AS (
	SELECT * FROM "Processed_Layer"."HCL_Processed_Data" WHERE  "Date" = (SELECT MAX("Date") FROM "Processed_Layer"."HCL_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."HCL_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."HCL_Processed_Data")))
),
min_price AS (
	SELECT * FROM "Processed_Layer"."HCL_Processed_Data" WHERE  "Date" = (SELECT MIN("Date") FROM "Processed_Layer"."HCL_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."HCL_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."HCL_Processed_Data")))
)
SELECT 'HCL' AS "Stock", b."Date" AS min_date, b."Price" AS min_price, a."Date" AS max_date, a."Price" AS max_price, ROUND((((a."Price" - b."Price")/b."Price")*100),2) 
		FROM max_price a CROSS JOIN min_price b;
END IF;
IF ((SELECT MAX("Month_Id") FROM "Processed_Layer"."TCS_Processed_Data") >= 11) THEN
INSERT INTO "Collective_Layer"."Collective_Twelve_Month_Data"
WITH max_price AS (
	SELECT * FROM "Processed_Layer"."TCS_Processed_Data" WHERE  "Date" = (SELECT MAX("Date") FROM "Processed_Layer"."TCS_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."TCS_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."TCS_Processed_Data")))
),
min_price AS (
	SELECT * FROM "Processed_Layer"."TCS_Processed_Data" WHERE  "Date" = (SELECT MIN("Date") FROM "Processed_Layer"."TCS_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."TCS_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."TCS_Processed_Data")))
)
SELECT 'TCS' AS "Stock", b."Date" AS min_date, b."Price" AS min_price, a."Date" AS max_date, a."Price" AS max_price, ROUND((((a."Price" - b."Price")/b."Price")*100),2) 
		FROM max_price a CROSS JOIN min_price b;
END IF;		
IF ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Wipro_Processed_Data") >= 11) THEN
INSERT INTO "Collective_Layer"."Collective_Twelve_Month_Data"
WITH max_price AS (
	SELECT * FROM "Processed_Layer"."Wipro_Processed_Data" WHERE  "Date" = (SELECT MAX("Date") FROM "Processed_Layer"."Wipro_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Wipro_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Wipro_Processed_Data")))
),
min_price AS (
	SELECT * FROM "Processed_Layer"."Wipro_Processed_Data" WHERE  "Date" = (SELECT MIN("Date") FROM "Processed_Layer"."Wipro_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Wipro_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Wipro_Processed_Data")))
)
SELECT 'Wipro' AS "Stock", b."Date" AS min_date, b."Price" AS min_price, a."Date" AS max_date, a."Price" AS max_price, ROUND((((a."Price" - b."Price")/b."Price")*100),2) 
		FROM max_price a CROSS JOIN min_price b;
END IF;
IF ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Infosys_Processed_Data") >= 11) THEN
INSERT INTO "Collective_Layer"."Collective_Twelve_Month_Data"
WITH max_price AS (
	SELECT * FROM "Processed_Layer"."Infosys_Processed_Data" WHERE  "Date" = (SELECT MAX("Date") FROM "Processed_Layer"."Infosys_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Infosys_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Infosys_Processed_Data")))
),
min_price AS (
	SELECT * FROM "Processed_Layer"."Infosys_Processed_Data" WHERE  "Date" = (SELECT MIN("Date") FROM "Processed_Layer"."Infosys_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Infosys_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Infosys_Processed_Data")))
)
SELECT 'Infosys' AS "Stock", b."Date" AS min_date, b."Price" AS min_price, a."Date" AS max_date, a."Price" AS max_price, ROUND((((a."Price" - b."Price")/b."Price")*100),2) 
		FROM max_price a CROSS JOIN min_price b;
END IF;		
IF ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Tech_Mahindra_Processed_Data") >= 11) THEN
INSERT INTO "Collective_Layer"."Collective_Twelve_Month_Data"
WITH max_price AS (
	SELECT * FROM "Processed_Layer"."Tech_Mahindra_Processed_Data" WHERE  "Date" = (SELECT MAX("Date") FROM "Processed_Layer"."Tech_Mahindra_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Tech_Mahindra_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Tech_Mahindra_Processed_Data")))
),
min_price AS (
	SELECT * FROM "Processed_Layer"."Tech_Mahindra_Processed_Data" WHERE  "Date" = (SELECT MIN("Date") FROM "Processed_Layer"."Tech_Mahindra_Processed_Data" WHERE "Month_Id" IN ((SELECT MAX("Month_Id") FROM "Processed_Layer"."Tech_Mahindra_Processed_Data"),(SELECT (MAX("Month_Id")-11) FROM "Processed_Layer"."Tech_Mahindra_Processed_Data")))
)
SELECT 'Tech_Mahindra' AS "Stock", b."Date" AS min_date, b."Price" AS min_price, a."Date" AS max_date, a."Price" AS max_price, ROUND((((a."Price" - b."Price")/b."Price")*100),2) 
		FROM max_price a CROSS JOIN min_price b;
END IF;		
END;		
$$
