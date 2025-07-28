CREATE OR REPLACE PROCEDURE "Consumable_Layer"."Stored_Procedure_Load_TCS_Processed_to_MonthlyDataDif"()
language plpgsql
as
$$
DECLARE i INTEGER;
DECLARE j INTEGER;
BEGIN
CREATE  TEMP TABLE counter AS 
(SELECT ROW_NUMBER() OVER(),"Date","Monthly_Label", "Price" FROM "Consumable_Layer"."TCS_Monthly_Data");
i := 1;
j := 2;
TRUNCATE TABLE "Consumable_Layer"."TCS_Monthly_Data_Diff";
WHILE (j<=(SELECT COUNT(*) FROM "Consumable_Layer"."TCS_Monthly_Data" )) LOOP
INSERT INTO "Consumable_Layer"."TCS_Monthly_Data_Diff" 
SELECT DISTINCT (SELECT "Date" FROM counter WHERE row_number = j),
CONCAT ((SELECT "Monthly_Label" FROM counter WHERE row_number = i),' - ',(SELECT "Monthly_Label" FROM counter WHERE row_number = j), ' Perc Diff'),
	ROUND(((((SELECT "Price" FROM counter WHERE row_number = j) - (SELECT "Price" FROM counter WHERE row_number = i))/(SELECT "Price" FROM counter WHERE row_number = i))*100),2) 
	;
	j := j+1;
	i := i+1;
END LOOP;
drop table counter;
END LOOP;
drop table counter;
END;
$$;
