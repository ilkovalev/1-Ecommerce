import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'i.kovalev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 20),
    'schedule_interval': '0 12 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

@dag(default_args=default_args, catchup=False)
def games_analytics_IK_lesson3():
    @task(retries=3)
    def get_data():
        data = pd.read_csv(url)
        year = 1994 + hash(f'i-kovalev-32') % 23
        data = data.query('Year == @year')
        return data

    @task(retries=4, retry_delay=timedelta(10))
    def top_selling_game(data):
        game = data.groupby('Name', as_index = False).agg({'Global_Sales':'sum'}).nlargest(1,'Global_Sales').Name.values[0]
        return game

    @task()
    def top_selling_genre_eu(data):
        genre = data.groupby('Genre', as_index = False).agg({'EU_Sales':'sum'}).nlargest(1,'EU_Sales').Genre.values[0]
        return genre

    @task()
    def top_selling_platform_us(data):
        platform = data.groupby('Platform', as_index = False).agg({'NA_Sales':'sum','Name':'nunique'}).query('NA_Sales > 1').nlargest(1,'Name').Platform.values[0]
        return platform

    @task()
    def top_publisher_jp(data):
        publisher = data.groupby('Publisher', as_index = False).agg({'JP_Sales':'sum'}).nlargest(1,'JP_Sales').Publisher.values[0]
        return publisher
    
    @task()
    def eu_vs_jp_games(data):
        eu_vs_jp = data.groupby('Name', as_index = False).agg({'EU_Sales':'sum','JP_Sales':'sum'}).query('EU_Sales > JP_Sales ').Name.nunique()
        return eu_vs_jp

#     @task(on_success_callback=send_message)
#     def print_data():
#         year = 1994 + hash(f'i-kovalev-32') % 23
#         print(f'''Game analytics for {year} year''')

    data = get_data()
    
    top_selling_game = top_selling_game(data)
    top_selling_genre_eu = top_selling_genre_eu(data)
    top_selling_platform_us = top_selling_platform_us(data)
    top_publisher_jp = top_publisher_jp(data)    
    eu_vs_jp_games = eu_vs_jp_games(data)    
#     print_data()

games_analytics_IK_lesson3 = games_analytics_IK_lesson3()