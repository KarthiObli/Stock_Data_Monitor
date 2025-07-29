from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

with DAG(
        dag_id = 'Wipro',
        start_date = datetime(2025,7,5),
        catchup = False
    )   as dag:
    task_1 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Raw_to_Processed",
        conn_id = "postgres_localhost",
        sql = """CALL "Processed_Layer"."Stored_Procedure_Load_Wipro_Raw_to_Processed"()"""
    )
    task_2 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_Daily_Data_L3M",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_Daily_Data_L3M"()"""
    )
    task_3 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_Daily_Data_QTD",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_Daily_Data_QTD"()"""
    )
    task_4 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_Monthly_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_Monthly_Data"()"""
    )
    task_5 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_MonthlyDataAvg",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_MonthlyDataAvg"()"""
    )
    task_6 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_Weekly_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_Weekly_Data"()"""
    )
    task_7 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_Weekly_DataAvg",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_Weekly_DataAvg"()"""
    )
    task_8 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_MonthlyDataDif",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_MonthlyDataDif"()"""
    )
    task_9 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Wipro_Processed_to_WeeklyDataDiff",
        conn_id = "postgres_localhost",
        sql = """CALL "Consumable_Layer"."Stored_Procedure_Load_Wipro_Processed_to_WeeklyDataDiff"()"""
    )
    task_10 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Collective_One_Month_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Collective_Layer"."Stored_Procedure_Load_Collective_One_Month_Data"()"""
    )
    task_11 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Collective_Three_Month_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Collective_Layer"."Stored_Procedure_Load_Collective_Three_Month_Data"()"""
    )
    task_12 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Collective_Six_Month_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Collective_Layer"."Stored_Procedure_Load_Collective_Six_Month_Data"()"""
    )
    task_13 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Collective_Nine_Month_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Collective_Layer"."Stored_Procedure_Load_Collective_Nine_Month_Data"()"""
    )
    task_14 = SQLExecuteQueryOperator(
        task_id = "Run_Stored_Procedure_Load_Collective_Twelve_Month_Data",
        conn_id = "postgres_localhost",
        sql = """CALL "Collective_Layer"."Stored_Procedure_Load_Collective_Twelve_Month_Data"()"""
    )
    task_1 >> [task_2, task_3, task_4, task_5, task_6, task_7]
    task_4 >> task_8
    task_5 >> task_9
    task_1 >> [task_10,task_11,task_12,task_13,task_14]
