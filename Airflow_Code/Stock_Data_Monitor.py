from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='Stock_Data_Monitor',
    start_date=datetime(2025, 7, 5, 20),
    schedule_interval= '@monthly',
    catchup=False,
) as dag:

    HCL = TriggerDagRunOperator(
        task_id='Trigger_HCL',
        trigger_dag_id='HCL',
        wait_for_completion=True
    )
    Infosys = TriggerDagRunOperator(
        task_id='Trigger_Infosys',
        trigger_dag_id='Infosys',
        wait_for_completion=True
    )    
    TCS = TriggerDagRunOperator(
        task_id='Trigger_TCS',
        trigger_dag_id='TCS',
        wait_for_completion=True
    )    
    Tech_Mahindra = TriggerDagRunOperator(
        task_id='Trigger_TechMahindra',
        trigger_dag_id='Tech_Mahindra',
        wait_for_completion=True
    )
    Wipro = TriggerDagRunOperator(
        task_id='Trigger_Wipro',
        trigger_dag_id='Wipro',
        wait_for_completion=True
    )
    [HCL,Infosys,TCS,Tech_Mahindra,Wipro]
