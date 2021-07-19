from airflow import DAG
from datetime import datetime
from airflow.operators import PythonOperator

default_args = {
    'owner': 'vloseva',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 12),
    'retries': 0
}

dag = DAG('report_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval='31 17 * * 1')


def report_vk_mondays_airflow():
    import pandas as pd
    import vk_api
    import random

    path = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR' \
           '-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single' \
           '=true&output=csv'
    df = pd.read_csv(path)
    # calculate views per day
    df_view = df.query('event == "view"') \
        .groupby('date', as_index=False) \
        .agg({'event': 'count'}) \
        .rename(columns={'event': 'views'})
    # calculate clicks per day
    df_click = df.query('event == "click"') \
        .groupby('date', as_index=False) \
        .agg({'event': 'count'}) \
        .rename(columns={'event': 'clicks'})
    # merge into oene df
    df_metrics = df_view.merge(df_click)
    # calculate CTR by day
    df_metrics['CTR'] = df_metrics.clicks / df_metrics.views * 100
    # calculate expenses by day
    df_metrics['expenses'] = df.ad_cost / 1000 * df_metrics.views
    df_metrics = df_metrics.round(2)
    # difference between yesterday and today in %
    difference_percent = (df_metrics.loc[1, 'views':] - df_metrics.loc[0, 'views':]) \
                         / df_metrics.loc[0, 'views':] * 100
    # today (April, 2)
    today = df_metrics.loc[1]
    # report
    report = (f"Отчет по объявлению 121288 за {today.date}: \n"
              f"Траты: {today.expenses} рублей ({difference_percent.expenses.round()}%) \n"
              f"Показы: {today.views} ({difference_percent.views.round()}%) \n"
              f"Клики: {today.clicks} ({difference_percent.clicks.round()}%) \n"
              f"CTR: {today.CTR} ({difference_percent.CTR.round()}%)")
    # Token
    app_token = 'TOKEN'
    # id of my user-receiver
    my_id = 'YOURID'
    # Initialize session
    vk_session = vk_api.VkApi(token=app_token)
    # Make it possible to use vk api methods as python methods
    vk = vk_session.get_api()
    # send report via vk
    vk.messages.send(
        user_id=my_id,
        random_id=random.randint(1, 2 ** 31),
        message=report)


t1 = PythonOperator(task_id='report_vk',
                    python_callable=report_vk_mondays_airflow,
                    dag=dag)
