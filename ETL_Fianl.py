from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from Dag_Function import clean, convert,dailychange

def extract_data_from_tables(ti):
   
    # using postgres_hook for connecting to postgresql and get data with sql    

    hook = PostgresHook(postgres_conn_id='postgres_conn_source')

    query_table1 = "SELECT * FROM confirmed_source1;"
    query_table2 = "SELECT * FROM deaths_source2;"

    df_extract_confirmed = hook.get_pandas_df(query_table1)
    df_extract_deaths = hook.get_pandas_df(query_table2)

    print(f'There are {len(df_extract_confirmed)} rows has integrated from the PosgreSQL.')
    print(f'There are {len(df_extract_deaths)} rows has integrated from the PosgreSQL.')

    csv_file_path = '~/airflow/dags/recovered_source_3.csv'
    df_extract_recovered = pd.read_csv(csv_file_path)
    
    print(f'There are {len(df_extract_recovered)} rows has integrated from the CSV.')


    ti.xcom_push(key='df_extract_confirmed', value=df_extract_confirmed)
    ti.xcom_push(key='df_extract_deaths', value=df_extract_deaths)
    ti.xcom_push(key='df_extract_recovered', value=df_extract_recovered)


def Transformation(ti):
    df_extract1 = ti.xcom_pull(task_ids='extract_data', key='df_extract_confirmed')
    df_extract2 = ti.xcom_pull(task_ids='extract_data', key='df_extract_deaths')
    df_extract3 = ti.xcom_pull(task_ids='extract_data', key='df_extract_recovered')

    print("Data has extracted successfully.")


    df_extract1_clean = clean(df=df_extract1)
    df_extract2_clean = clean(df=df_extract2)
    df_extract3_clean = clean(df=df_extract3)

    print("Data has cleaned successfully.")

    df_convert1 = convert(clean_df=df_extract1_clean)
    df_convert2 = convert(clean_df=df_extract2_clean)
    df_convert3 = convert(clean_df=df_extract3_clean)

    print("Data has converted successfully.")

    df_convert1_add_dailychanging = dailychange(convert_df=df_convert1)
    df_convert2_add_dailychanging = dailychange(convert_df=df_convert2)
    df_convert3_add_dailychanging = dailychange(convert_df=df_convert3)

    print(f'There are {len(df_convert1_add_dailychanging)} in df_final_confirmed in PosgreSQL.')
    print(f'There are {len(df_convert2_add_dailychanging)} in df_final_deaths in PosgreSQL.')
    print(f'There are {len(df_convert3_add_dailychanging)} in df_final_recovered in PosgreSQL.')

    
    ti.xcom_push(key='df_final_confirmed', value=df_convert1_add_dailychanging)
    ti.xcom_push(key='df_final_deaths', value=df_convert2_add_dailychanging)
    ti.xcom_push(key='df_final_recovered', value=df_convert3_add_dailychanging)
    
def load_to_postgres(ti):

    confirmed_final_df = ti.xcom_pull(task_ids='transform_task', key='df_final_confirmed')
    deaths_final_df = ti.xcom_pull(task_ids='transform_task', key='df_final_deaths')
    recovered_final_df = ti.xcom_pull(task_ids='transform_task', key='df_final_recovered')
    
    conn_id = "postgres_conn_covid_destenation"
    conn = BaseHook.get_connection(conn_id)
    conn_uri = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    # Create SQLAlchemy engine
    engine = create_engine(conn_uri)

    # Use df.to_sql() to insert data into PostgreSQL
    confirmed_final_df.to_sql('covid_19_confirmed_1', engine, if_exists='replace', index=False)
    deaths_final_df.to_sql('covid_19_deaths_2', engine, if_exists='replace', index=False)
    recovered_final_df.to_sql('covid_19_recovered_3', engine, if_exists='replace', index=False)

    print("Data has been loaded into the table successfully.")

default_args = {
    'owner': 'mostafa',
    'start_date': datetime(2023, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ETL_Dag',
    default_args=default_args,
    description='the final project',
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_tables,
    dag=dag,
)


transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=Transformation,
    dag=dag,
)

create_table_in_postgresql = PostgresOperator(  

    task_id='create_table_in_psql', 
    postgres_conn_id='postgres_conn_covid_destenation',
    sql="""
    CREATE TABLE covid_19_confirmed_1 (
    country VARCHAR(100),
    date DATE,
    n_of_confirmed INTEGER,
    dailychange_confirmed INTEGER
    );

    CREATE TABLE covid_19_deaths_2 (
    country VARCHAR(100),
    date DATE,
    n_of_deaths INTEGER,
    dailychange_deaths INTEGER
    );

    CREATE TABLE covid_19_recovered_3 (
    country VARCHAR(100),
    date DATE,
    n_of_recovered INTEGER,
    dailychange_recovered INTEGER
    );
    """,  
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres,
    dag=dag,
)

extract_task >> transform_task >> create_table_in_postgresql >> load_task
