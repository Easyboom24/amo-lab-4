from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import random
import string
from multiprocessing import Pool


# Параметры
NUM_FILES = 100
OUTPUT_DIR = './output_files'  # Укажите путь к папке для файлов

# Функция для создания 100 текстовых файлов
def create_text_files():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for i in range(NUM_FILES):
        content = ''.join(random.choices(string.ascii_lowercase, k=1000))
        with open(os.path.join(OUTPUT_DIR, f'{i}.txt'), 'w') as f:
            f.write(content)

# Функция для подсчета символов 'a' в файле
def count_a_in_file(file_num):
    with open(os.path.join(OUTPUT_DIR, f'{file_num}.txt'), 'r') as f:
        content = f.read()
    count = content.count('a')
    with open(os.path.join(OUTPUT_DIR, f'{file_num}.res'), 'w') as res_file:
        res_file.write(str(count))

# Обертка для параллельного запуска
def count_a_parallel():
    with Pool(NUM_FILES) as p:
        p.map(count_a_in_file, range(NUM_FILES))

# Функция для суммирования всех результатов
def aggregate_results():
    total_count = 0
    for i in range(NUM_FILES):
        with open(os.path.join(OUTPUT_DIR, f'{i}.res'), 'r') as res_file:
            total_count += int(res_file.read())
    with open(os.path.join(OUTPUT_DIR, 'total_count.txt'), 'w') as total_file:
        total_file.write(f'Total count of "a": {total_count}')
    print(total_count)
    return total_count

# Определение DAG
args = {
    'owner': 'airflow',
    'start_date':datetime(2023, 10, 1),
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'depends_on_past': False,
}
with DAG('text_processing_dag', default_args=args, schedule_interval=None) as dag:

    # Задача для создания текстовых файлов
    create_files_task = PythonOperator(
        task_id='create_text_files',
        python_callable=create_text_files,
        dag=dag
    )

    # Задача для параллельного подсчета символов 'a'
    count_a_task = PythonOperator(
        task_id='count_a_in_files',
        python_callable=count_a_parallel,
        dag=dag
    )

    # Задача для суммирования результатов
    aggregate_task = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        dag=dag
    )

    # Установка порядка выполнения задач
    create_files_task >> count_a_task >> aggregate_task
