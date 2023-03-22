import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df[['domain_name', 'TLD']] = top_data_df['domain'].str.split(r'\.(?=[^\.]+$)', n=1, expand=True)
    top_data_df.groupby('TLD',as_index = False).agg({'domain_name':'nunique'}).sort_values('domain_name', ascending = False).head(10) 
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_length_domain = top_data_df.loc[top_data_df.domain_name.str.len().idxmax()].domain_name
    with open('longest_length_domain.csv', 'w') as f:
        f.write(longest_length_domain.to_csv(index=False, header=False))

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')['rank'].values
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))        
        

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data_10 = f.read()
    with open('longest_length_domain.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_rank = f.read()
        
    print('Top 10 domains')
    print(all_data_10)

    print('Domain with longest name')
    print(all_data_len)
    
    print('Airflow Rank')
    print(all_data_rank)


default_args = {
    'owner': 'i-kovalev-32',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 14),
}
schedule_interval = '0 16 * * *'

i_kovalev_32 = DAG('top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=i_kovalev_32)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=i_kovalev_32)

t2_com = PythonOperator(task_id='get_stat_com',
                        python_callable=get_stat_com,
                        dag=i_kovalev_32)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=i_kovalev_32)

t1 >> [t2, t2_com] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)