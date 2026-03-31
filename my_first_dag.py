import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def download_top_domains():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_doms.to_csv(TOP_1M_DOMAINS_FILE, index=False)

def filter_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['rank'] = pd.to_numeric(top_data_df['rank'], errors='coerce')
    top_data_top_10 = top_data_df[top_data_df['rank'] <= 10]

    top_data_top_10.to_csv('top_data_top_10.csv', index=False, header=False)

def find_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_name = top_data_df.loc[top_data_df['domain'].astype(str).str.len().idxmax(), 'domain']

    with open('max_domain_name.txt', 'w') as f:
        f.write(str(max_name))

def check_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_row = top_data_df[top_data_df['domain'] == "airflow.com"]
    if not airflow_row.empty:
        placement = str(airflow_row['rank'].values[0])
    else:
        placement = "Airflow.com not found"
    with open('airflow_placement.txt', 'w') as f:
        f.write(placement)

def display_results(ds, **kwargs):
    with open('top_data_top_10.csv', 'r') as f:
        all_data_10 = f.read()
    with open('max_domain_name.txt', 'r') as f:
        max_domain = f.read()
    with open('airflow_placement.txt', 'r') as f:
        airflow_rank = f.read()
        
    print(f'Top domains for date {ds}')
    print(all_data_10)
    print(f'The longest domain name for date {ds}')
    print(max_domain)
    print(f'The airflow placement for date {ds}')
    print(airflow_rank)

default_args = {
    'owner': 'a.nikolaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 7),
}

dag = DAG('unique_domain_analytics_nikolaeva',
          default_args=default_args, 
          schedule_interval='15 13 * * *')


t1 = PythonOperator(task_id='load_csv_data',
                    python_callable=download_top_domains, 
                    dag=dag)

t2 = PythonOperator(task_id='calc_top_10', 
                    python_callable=filter_top_10_domains, 
                    dag=dag)

t2_max = PythonOperator(task_id='calc_longest_name', 
                        python_callable=find_longest_domain, 
                        dag=dag)

t2_airflow = PythonOperator(task_id='calc_airflow_pos',
                            python_callable=check_airflow_rank,
                            dag=dag)

t3 = PythonOperator(task_id='log_final_stats',
                    python_callable=display_results, 
                    dag=dag)

t1 >> [t2, t2_max, t2_airflow] >> t3
