from src.components.extract import scrape_review, scrape_toko, scrape, scrape_review_toko, scrape_product_link
from src.logger import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import numpy as np


BUCKET_NAME = 'promag-tokopedia-data'


def delete_local_csv_files():
    files_to_delete = [f for f in os.listdir() if f.endswith('.csv')]
    for f in files_to_delete:
        os.remove(f)



def retrieve_products(ti, dag_run):
    nama_produk = dag_run.conf.get('nama_produk')
    jumlah_halaman = dag_run.conf.get('pages')
    print(jumlah_halaman)
    data = scrape.run(nama_produk, jumlah_halaman, True)
    data.to_csv(nama_produk+'.csv', index=False)
    ti.xcom_push(value=f'{nama_produk}.csv', key='file_name')
    ti.xcom_push(value=nama_produk, key='prod')

def retrieve_product_info(ti):
    file_name = ti.xcom_pull(key='file_name', task_ids='extract_product')#[0]
    links = pd.read_csv(file_name)['link'].values
    data = pd.DataFrame()
    driver = None
    for link in links:
        driver,data_dict =  scrape_product_link.run(link, True, driver)
        data = pd.concat([data, data_dict])
    data.to_csv(file_name[:-4]+'_info.csv', index=False)
    logging.info(data)
    driver.quit()
    ti.xcom_push(value=file_name[:-4]+'_info.csv', key='file_name')


def retrieve_product_reviews(ti):
    file_name = ti.xcom_pull(key='file_name', task_ids='extract_product')#[0]
    links = pd.read_csv(file_name)['link'].values
    data = pd.DataFrame()
    for link in links:
        data_dict = scrape_review.run(link, 3, True)
        data = pd.concat([data, data_dict])
    data.to_csv(file_name[:-4]+"_review.csv", index=False)
    ti.xcom_push(value=file_name[:-4]+'_review.csv', key='file_name')


def transform_product_data(ti):
    prod, info = ti.xcom_pull(key='file_name', task_ids=['extract_product', 'extract_info'])
    df = pd.read_csv(prod)
    df_inf = pd.read_csv(info)

    df.rename(columns={"name": "product"}, inplace=True)
    df.drop(columns=['sold', 'seller', 'product'], inplace=True)

    def convert_link(link):
        if link[9] == 'a':
            link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
        else :
            link = link.split("?extP")[0]
        return link
    
    df['link'] = df['link'].apply(convert_link)

    # df['id'] = df['seller'] + '/' + df['link']
    # df_rev['id'] = df_rev['seller'] + '/' + df_rev['link']
    # df_inf['id'] = df_inf['seller'] + '/' + df_inf['link']

    # d['seller']

    df = pd.merge(df, df_inf, on='link')#, df_rev, on ='link')


    df['rating'] = df['rating'].fillna(0).apply(float)
    df['price'] = df['price'].apply(lambda x:float(x[2:].replace('.','')))
    for i, row in df[['cashback(%)', 'price']].iterrows():
        if row['cashback(%)'] is np.nan:
            df['cashback(%)'][i] = 0.
        elif row['cashback(%)'].split(" ")[-1] == 'rb':
            df['cashback(%)'][i] = int(row['cashback(%)'].split(" ")[1]) / row['price']
        else :
            df['cashback(%)'][i] = float(row['cashback(%)'][9:-1])/100
    # df['cashback(%)'] = df['cashback(%)'].apply(lambda x: 0. if x is np.nan else float(x[9:-1])/100)
    
    # df['date'] = datetime.now()
    df['date'] = ti.execution_date
    df['search_keyword'] = ti.xcom_pull(task_ids='extract_product', key='prod')
    df[['total_rating','total_comment']] = df[['total_rating','total_comment']].fillna(0).astype('int32')

    print(df)
    logging.info(df)
    
    file_name = 'merged_'+ti.execution_date.strftime('%Y-%m-%d-%H-%M')+'_'+prod
    df.to_csv(file_name, index=False)
    ti.xcom_push(key='file_name', value= file_name)


def transform_review_data(ti):
    file_name = ti.xcom_pull(key='file_name', task_ids='extract_review')
    df = pd.read_csv(file_name)
    # df['rating'].dropna(inplace=True)
    print("Null Rating :", df[df['rating'].isna()].shape,"Null Date:", df[df['date'].isna()].shape,"Total Data:",df.shape)
    df.dropna(subset=['rating', 'date'], inplace=True)
    df['rating'] = df['rating'].apply(lambda x:int(x[-1]))#.astype(float)
    df['variant'] = df['variant'].apply(lambda x:x[8:] if x is not np.nan else x)
    # print(df[df['date'].isna()].shape, df.shape)

    def convert_date(x):
        now = datetime.now()
        a = x.split(" ")
        # a[0] = a[0].lower()
        # a[1] = a[1].lower()
        if a[0][0] == 'H':
            b = now
        elif a[1][0] == 'h':
            b = now - timedelta(days=int(a[0]))
        elif a[1][0] == 'm':
            b = now - timedelta(weeks=int(a[0]))
        elif a[1][0] == 'b':
            b = now - timedelta(days=int(a[0])*32 + now.second//2)
        elif a[3][0] == 't':
            b = now - timedelta(days=int(a[2])*366 + now.second*2)
        return b#.strftime('%Y-%m-%d')


    df['date'] = df['date'].apply(convert_date)
    df['search_keyword'] = ti.xcom_pull(task_ids='extract_product', key='prod')
    
    # df['total_rating'] = df['total_rating'].astype(int)
    file_name = file_name[:-4]+'_'+ti.execution_date.strftime('%Y-%m-%d-%H-%M')+'.csv'
    df.to_csv(file_name, index=False)
    ti.xcom_push(key='file_name', value=file_name)


dag = DAG(
    'tokped_etl',
    start_date = datetime(2023, 12,4),
)

extract_products = PythonOperator(
    task_id = 'extract_product',
    python_callable=retrieve_products,
    # op_kwargs={'nama_produk':'laptop','jumlah_halaman':10, 'headless':True},
    provide_context=True,
    dag=dag
)

extract_reviews = PythonOperator(
    task_id = 'extract_review',
    python_callable=retrieve_product_reviews,
    provide_context=True,
    dag=dag
)

extract_info = PythonOperator(
    task_id = 'extract_info',
    python_callable=retrieve_product_info,
    provide_context=True,
    default_args={'retries':3},# 'retry_delay':timedelta(seconds=5)},
    dag=dag
)

transform_product_data_task = PythonOperator(
    task_id = 'transform_product_data',
    python_callable=transform_product_data,
    provide_context=True,
    dag=dag
)

transform_review_data_task = PythonOperator(
    task_id = 'transform_review_data',
    python_callable=transform_review_data,
    provide_context=True,
    dag=dag
)


csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="csv_to_gcs",
        # src="{{ ti.xcom_pull(key='file_name', task_ids='extract_product') }}",
        src="{{ ti.xcom_pull(key='file_name', task_ids='transform_product_data') }}",#"merged_{{ ti.execution_date.strftime('%Y-%m-%d-%H-%M') }}_{{ ti.xcom_pull(key='file_name', task_ids='extract_product') }}",
        # dst=f'gs://{BUCKET_NAME}//'+"{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/",
        dst = "{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/",
        bucket=BUCKET_NAME,
)

csv_to_gcs_review = LocalFilesystemToGCSOperator(
        task_id="csv_to_gcs_review",
        src="{{ ti.xcom_pull(key='file_name', task_ids='transform_review_data') }}",
        # dst=f'gs://{BUCKET_NAME}//'+"{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/",
        dst = "{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/",
        bucket=BUCKET_NAME,
)

delete_csv = PythonOperator(
    task_id='delete_local_csv_files',
    python_callable=delete_local_csv_files,
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=BUCKET_NAME,
    source_objects=["{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/{{ ti.xcom_pull(task_ids='transform_product_data', key='file_name') }}"],
    # destination_project_dataset_table = "promag_tokopedia_data.{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}_product",
    destination_project_dataset_table = "promag_tokopedia_data.products",
    write_disposition='WRITE_APPEND'
)

gcs_to_bigquery_review = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_review',
    bucket=BUCKET_NAME,
    source_objects=["{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}/{{ ti.xcom_pull(task_ids='transform_review_data', key='file_name') }}"],
    # destination_project_dataset_table = "promag_tokopedia_data.{{ ti.xcom_pull(task_ids='extract_product', key='prod') }}_review",
    destination_project_dataset_table = "promag_tokopedia_data.reviews",
    write_disposition='WRITE_APPEND'
)

extract_products >> [extract_reviews, extract_info] #>> [transform_product_data_task, transform_review_data_task] >> csv_to_gcs >> delete_csv
extract_reviews >> transform_review_data_task >> csv_to_gcs_review >> gcs_to_bigquery_review
extract_info >> transform_product_data_task >> csv_to_gcs >> gcs_to_bigquery
[csv_to_gcs, csv_to_gcs_review] >> delete_csv

# extract_products >> extract_info >> transform_product_data_task >> csv_to_gcs >> delete_csv

# extract_products >> csv_to_gcs >> delete_csv >> gcs_to_bigquery