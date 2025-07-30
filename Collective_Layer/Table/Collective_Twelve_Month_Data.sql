CREATE TABLE IF NOT EXISTS "Collective_Layer"."Collective_Twelve_Month_Data"
(
    "Stock" character varying NOT NULL,
    "Min_Date" date NOT NULL,
    "Min_Price" numeric(7,2) NOT NULL,
    "Max_Date" date NOT NULL,
    "Max_Price" numeric(7,2) NOT NULL,
    "Percentage_Difference" numeric(7,2) NOT NULL
);
