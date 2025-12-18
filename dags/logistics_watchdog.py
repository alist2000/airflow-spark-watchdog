from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os
from fpdf import FPDF

# Spark Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_timestamp, datediff

# --- CONFIGURATION ---
DATA_PATH = '/opt/airflow/data/olist_orders_dataset.csv'
# WE WRITE TO TMP TO GUARANTEE SUCCESS (Bypassing Windows Permissions)
PDF_OUTPUT_DIR = '/tmp/' 
THRESHOLD_DAYS = 3 

def calculate_delivery_kpi(**kwargs):
    spark = SparkSession.builder \
        .appName("LogisticsWatchdog") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Could not find dataset at {DATA_PATH}")

    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    
    df = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    df = df.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    
    df_delivered = df.filter(
        (col("order_status") == "delivered") & 
        col("order_delivered_customer_date").isNotNull()
    )
    
    df_calc = df_delivered.withColumn(
        "delivery_days", 
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
    
    result_row = df_calc.agg(avg("delivery_days")).collect()
    result = result_row[0][0]
    spark.stop()
    
    print(f"Average Delivery Time: {result} days")
    kwargs['ti'].xcom_push(key='avg_delivery_days', value=result)

def check_threshold(**kwargs):
    ti = kwargs['ti']
    avg_days = ti.xcom_pull(key='avg_delivery_days', task_ids='calculate_metrics')
    
    if avg_days and avg_days > THRESHOLD_DAYS:
        return 'generate_alert_pdf'
    else:
        return 'generate_success_pdf'

def generate_alert_pdf(**kwargs):
    ti = kwargs['ti']
    avg_days = ti.xcom_pull(key='avg_delivery_days', task_ids='calculate_metrics')
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=16)
    
    pdf.cell(200, 10, txt="CRITICAL ALERT: LOGISTICS DELAY", ln=1, align='C')
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Date: {datetime.now().strftime('%Y-%m-%d')}", ln=1, align='C')
    pdf.ln(20)
    
    pdf.set_text_color(255, 0, 0)
    pdf.cell(200, 10, txt=f"Current Average Delivery Time: {round(avg_days, 2)} days", ln=1)
    pdf.set_text_color(0, 0, 0)
    
    pdf.cell(200, 10, txt=f"Threshold Limit: {THRESHOLD_DAYS} days", ln=1)
    pdf.cell(200, 10, txt="Action Required: Immediate investigation into Zone 4 logistics.", ln=1)
    
    # Save directly to /tmp
    filename = f"{PDF_OUTPUT_DIR}ALERT_REPORT_{datetime.now().strftime('%Y%m%d')}.pdf"
    pdf.output(filename)
    
    print(f"PDF Generated successfully at: {filename}")

def generate_success_pdf(**kwargs):
    ti = kwargs['ti']
    avg_days = ti.xcom_pull(key='avg_delivery_days', task_ids='calculate_metrics')
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=16)
    
    pdf.cell(200, 10, txt="Daily Operations Report: Normal", ln=1, align='C')
    pdf.set_font("Arial", size=12)
    pdf.ln(20)
    
    pdf.set_text_color(0, 128, 0)
    pdf.cell(200, 10, txt=f"Current Average Delivery Time: {round(avg_days, 2)} days", ln=1)
    
    # Save directly to /tmp
    filename = f"{PDF_OUTPUT_DIR}DAILY_REPORT_{datetime.now().strftime('%Y%m%d')}.pdf"
    pdf.output(filename)
    
    print(f"PDF Generated successfully at: {filename}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'logistics_watchdog',
    default_args=default_args,
    description='Spark + Airflow + PDF Reporting',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    calculate_metrics = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_delivery_kpi,
    )

    branch_task = BranchPythonOperator(
        task_id='check_threshold',
        python_callable=check_threshold,
    )

    create_alert = PythonOperator(
        task_id='generate_alert_pdf',
        python_callable=generate_alert_pdf,
    )

    create_report = PythonOperator(
        task_id='generate_success_pdf',
        python_callable=generate_success_pdf,
    )

    calculate_metrics >> branch_task >> [create_alert, create_report]