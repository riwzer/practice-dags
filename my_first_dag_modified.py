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

GAMES_STATS = 'https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv'
YEAR = 2009

default_args = {
    'owner': 'a.nikolaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '15 13 * * *'
}


@dag(default_args=default_args, catchup=False)
def get_games_stats():
    @task(retries=2)
    def get_data():
        games_stats = pd.read_csv(GAMES_STATS)
        return games_stats[games_stats['Year'] == YEAR].to_csv(index=False)
    
    @task()
    def most_profitable_game_of_the_year(games_stats_csv):
        games_stats = pd.read_csv(StringIO(games_stats_csv))
        answer_1 = games_stats[games_stats['Global_Sales'] == games_stats['Global_Sales'].max()].Name.unique()
        return answer_1.tolist() 
    
    @task()
    def most_profitable_genres_in_eu(games_stats_csv):
        games_stats = pd.read_csv(StringIO(games_stats_csv))
        answer_2 = games_stats[games_stats['EU_Sales'] == games_stats['EU_Sales'].max()].Genre.unique()
        return answer_2.tolist()
    
    @task()
    def best_platforms_in_na(games_stats_csv):
        games_stats = pd.read_csv(StringIO(games_stats_csv))
        answer_3 = games_stats[games_stats['NA_Sales'] > 1.0].groupby('Platform').agg({'Name': 'nunique'}).idxmax().tolist()
        return answer_3
    
    @task()
    def the_best_publisher_in_jp(games_stats_csv):
        games_stats = pd.read_csv(StringIO(games_stats_csv))
        answer_4 = games_stats.groupby('Publisher').agg({'JP_Sales': 'mean'}).idxmax().tolist()
        return answer_4
    
    @task()
    def sold_better_in_eu(games_stats_csv):
        games_stats = pd.read_csv(StringIO(games_stats_csv))
        # Исправлено с data на games_stats
        answer_5 = int(len(games_stats[games_stats['EU_Sales'] > games_stats['JP_Sales']].Name.unique()))
        return answer_5

    @task()
    def print_games_stats(answer_1, answer_2, answer_3, answer_4, answer_5):
        print(f'Самая продаваемая игра на момент {YEAR} года: ', *answer_1)
        print(f'Самые продаваемые жанры игр в Европе на момент {YEAR} года: ', *answer_2)
        print(f'Платформа с наибольшим количеством игр с более чем миллионным тиражом на момент {YEAR} года: ', *answer_3)
        print(f'Издательство с самыми большими средними продажами в Японии на момент {YEAR} года: ', *answer_4)
        print(f'Число игр, которые продавались лучше в Европе, чем в Японии на момент {YEAR} года: ', answer_5)

        
    
    games_stats = get_data()
    answer_1 = most_profitable_game_of_the_year(games_stats)
    answer_2 = most_profitable_genres_in_eu(games_stats)
    answer_3 = best_platforms_in_na(games_stats)
    answer_4 = the_best_publisher_in_jp(games_stats)
    answer_5 = sold_better_in_eu(games_stats)
    
    print_games_stats(answer_1, answer_2, answer_3, answer_4, answer_5)

    
get_games_stats = get_games_stats()
    
    