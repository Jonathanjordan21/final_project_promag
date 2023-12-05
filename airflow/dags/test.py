# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.sensors.filesystem import FileSensor
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator,PubSubCreateSubscriptionOperator
# from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
# from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
# # from airflow.providers.google.cloud.operators
# # from airflow.providers.google.transfers.pubsub_to_bigquery import PubSubToBigQueryOperator
# from src.components.extract import scrape_review, scrape_product_link, scrape
# import pandas as pd



# # def retrieve_product_info(ti):
# #     file_name = ti.xcom_pull(key='file_name', task_ids='extract_product')#[0]
# #     links = pd.read_csv(file_name)['link'].values
# #     data = pd.DataFrame()
# #     driver = None
# #     for link in links:
# #         driver,data_dict =  scrape_product_link.run(link, True, driver)
# #         data = pd.concat([data, data_dict])
# #     data.to_csv(file_name[:-4]+'_info.csv', index=False)
# #     logging.info(data)
# #     driver.quit()
# #     ti.xcom_push(value=file_name[:-4]+'_info.csv', key='file_name')


# def retrieve_product_reviews(ti):
#     # file_name = ti.xcom_pull(key='file_name', task_ids='extract_product')#[0]
#     # links = pd.read_csv(file_name)['link'].values
#     a = ti.xcom_pull(task_ids='publish_links_to_pubsub')
#     print("huehue", a)
#     yield a

#     # for x in ti.xcom_pull(task_ids='wait_links', key='return_value'):
#     #     print(datetime.now())
#     #     print("euy retrieve ")
#     #     print(x['message']['attributes']['link'])
#     #     # yield x['message']['attributes']['link']

#     #     ti.xcom_push(key='test', value=x['message']['attributes']['link'] + "haha")


# def ext_dummy(ti):
#     print('hahe')
#     a = ti.xcom_pull(task_ids='extract_review')
#     print(a[:30] + 'hahe')
#     # ti.xcom_push(key='test', value=a[:30])
#     yield a[:30]

# def dummy2(ti):
#     print('huehue')
#     a = ti.xcom_pull(task_ids='ext_dum')
#     print(a[:6] + 'huehue')
#     # ti.xcom_push(key='test', value=a[:6]='huehue')
#     yield a[:6]


#     # print("euy")
#     # print(PubSubHook().pull( subscription=subscription, max_messages=1))

#     # {{ link }}
#     # a = ti.xcom_pull('pull_messages')
#     # print(a)
#     # print(list(a))
#     # print([x.get('message') for x in a])
#     # print(messages)
#     # print(messages['data'])
#     # data = pd.DataFrame()
#     # data.to_csv(file_name[:-4]+"_review.csv", index=False)
#     # ti.xcom_push(value=file_name[:-4]+'_review.csv', key='file_name')


# def retrieve_products(ti, dag_run):
#     nama_produk = dag_run.conf.get('nama_produk')
#     data = scrape.run(nama_produk, 1, True)
#     data.to_csv(nama_produk+'.csv', index=False)
#     ti.xcom_push(value=f'{nama_produk}.csv', key='file_name')
#     ti.xcom_push(value=nama_produk, key='prod')
#     # for x in PubSubHook().



# def publish_links_to_pubsub(ti):
#     project_id = 'msib-final-project'
#     topic = 'promag-tokopedia-link'

#     # Load links from CSV file
#     # print(file_name)
#     # print(kwargs)
#     # ti = kwargs['ti']
    
#     # file_name = ti.xcom_pull(task_ids='extract_product', key='file_name')
#     df = pd.read_csv('laptop.csv')

#     def convert_link(link):
#         if link[9] == 'a':
#             link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
#         else :
#             link = link.split("?extP")[0]
#         return link
    
#     df['link'] = df['link'].apply(convert_link)

#     for i, link in enumerate(df['link']):
#         print("yes", datetime.now())
#         # PubSubHook().publish(topic= topic, messages=[{"attributes": {"link":link}}], project_id=project_id)
#         yield link
#         print(f"Published link to Pub/Sub: {link}")


# default_args = {
#     'owner': 'airflow',
#     # 'start_date': datetime(2023, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(seconds=5),
# }

# dag = DAG(
#     'pubsub_link_processing_dags',
#     default_args=default_args,
#     start_date=datetime(2023,12,4),
#     schedule_interval=None,  # Define your schedule interval
# )

# # Use FileSensor to wait for the presence of the file
# # file_sensor_task = FileSensor(
# #     task_id='file_sensor_task',
# #     filepath='laptop.csv', 
# #     poke_interval=30,  
# #     timeout=600,  
# #     mode='poke',
# #     dag=dag,
# # )

# publish_links_task = PythonOperator(
#     task_id='publish_links_to_pubsub',
#     python_callable=publish_links_to_pubsub,
#     provide_context=True,
#     dag=dag,
# )

# # subscribe_task = PubSubCreateSubscriptionOperator(
# #     task_id='subscribe_and_process_links',
# #     project_id='msib-final-project',
# #     topic='promag-tokopedia-link',
# #     dag=dag,
# # )

# extract_reviews = PythonOperator(
#     task_id = 'extract_review',
#     python_callable=retrieve_product_reviews,
#     provide_context=True,
#     dag=dag
# )

# d = PythonOperator(
#     task_id = 'ext_dum',
#     python_callable=ext_dummy,
#     provide_context=True,
#     dag=dag
# )

# b = PythonOperator(
#     task_id = 'dummy2',
#     python_callable=dummy2,
#     provide_context=True,
#     dag=dag
# )

# # extract_products = PythonOperator(
# #     task_id = 'extract_product',
# #     python_callable=retrieve_products,
# #     # op_kwargs={'nama_produk':'laptop','jumlah_halaman':10, 'headless':True},
# #     provide_context=True,
# #     dag=dag
# # )


# # subscription = subscribe_task.output

# # sensor_task = PubSubPullSensor(
# #     task_id = 'wait_links',
# #     project_id = 'msib-final-project',
# #     subscription=subscription,
# #     ack_messages=True,
# #     dag=dag,
# #     max_messages=1,
# #     # deferrable=True
# # )

# # Set up the DAG execution order

# publish_links_task >> extract_reviews >> d >> b
