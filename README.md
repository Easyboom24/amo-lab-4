# АМО. Лабораторная работа № 4
## Выполнил: Шенцов Ярослав

# Установка и запуск Apache Airflow

Этот документ описывает процесс установки и запуска Apache Airflow, а также управление переменными окружения.

## Установка

1. **Создание виртуальной среды и Apache Airflow**

   Сначала создайте виртуальную среду для изоляции пакетов:

   ```bash
   python -m venv venv
   ```
   ```bash
   source myenv/bin/activate
   ```
   ```bash
   pip install apache-airflow
   ```

2. **Установка переменной AIRFLOW_HOME**
   ```bash
   export AIRFLOW_HOME=`pwd`/airflow_home
   ```

3. **Запуск Apache Airflow**
   ```bash
   airflow db init
   ```
   ```bash
   airflow webserver --port 8881
   ```
   ```bash
   airflow scheduler
   ```
  

