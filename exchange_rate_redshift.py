import logging
from datetime import datetime, timedelta
from functools import reduce
from tempfile import NamedTemporaryFile

import pandas as pd
import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

exchange_name = 'rates'


def get_exchange_rates(ds, accesskey, base='USD'):
    """
    Makes the api request for the exchange rate of all offered currencies to a base currency
    at a given date.

    :type ds: date or str
    :param ds: The execution date of the api. Expects date(ds) in the format YYYY-MM-DD.

    :type base: str
    :param base: The currency code of the base currency. Expects base currency to be the
        3 symbol currency code like 'USD'.
    """

    url = "https://api.exchangeratesapi.io/{}?base={}&access_key={}".format(ds, base, accesskey)
    logging.info(f"Calling {url}")
    return requests.get(url).json()


def get_data(ds, accesskey, base='USD'):
    """
    Gets all the data and stores it in arrays to be inserted into their tables.

    :type ds: date or str
    :param ds: The execution date of the api. Expects date(ds) in the format YYYY-MM-DD.

    :type base: str
    :param base: The currency code of the base currency. Expects base currency to be the
        3 symbol currency code like 'USD'.
    """

    data = get_exchange_rates(ds=ds, accesskey=accesskey, base=base)
    rates = []
    for d in data['rates'].keys():
        cur = [d, data['base'], ds, data['rates'][d],
               data['date'], "https://api.exchangeratesapi.io"]
        rates.append(cur)
    return rates


def csv_to_s3(ds, base='USD', **kwargs):
    """
    Saves the exchange rate data from the exchange rate api used in get_data as a csv in s3.

    :type ds: date or str
    :param ds: The execution date of the api. Expects date(ds) in the format YYYY-MM-DD.

    :type base: str
    :param base: The currency code of the base currency. Expects base currency to be the 3
        symbol currency code like 'USD'.
    """
    rates = get_data(ds=ds, accesskey=accesskey, base=base)
    logging.info(
        f"Received {len(rates)} rows from exchange rates api with date={ds} and base={base}.")
    with NamedTemporaryFile() as tempfile:

        pd.DataFrame(rates).to_csv(tempfile.name, index=False, header=False)

        key = "currency-exchange/{}_{}.csv".format(exchange_name, ds)
        s3 = S3Hook(aws_conn_id=connections.aws)
        s3.load_file(
            filename=tempfile.name,
            bucket_name="exchange_rates",
            key=key,
            replace=True)
        logging.info("Uploaded csv data to " + key)


def s3_to_redshift(ds, **kwargs):
    """
    Performs a COPY operation from a csv in s3 to the given table in redshift.

    :type ds: date or str
    :param ds: The execution date of the api. Expects date(ds) in the format YYYY-MM-DD.

    """

    # Getting the arguments for connection

    table = reference.exchange_rates

    pg = PostgresHook()
    copy_auth = RedshiftAuthorizationHook()

    with pg.get_conn() as connection:
        with connection.cursor() as cursor:
            key = "currency-exchange/{}_{}.csv".format(exchange_name, ds)

            columns = [
                table.currency_code,
                table.base_currency_code,
                table.exchange_rate_date,
                table.exchange_rate,
                table.reported_date,
                table.source]

            temp = reduce(lambda x, y: str(x) + ", " + str(y), columns)

            connection.autocommit = True
            cursor.execute("CREATE TEMP TABLE tmp (LIKE {})".format(table))
            cursor.execute(
                """
                COPY tmp({})
                FROM 's3://{}/{}'
                {}
                CSV;
                """.format(temp, "exhange_rates", key, copy_auth)
            )
            logging.info("Created temp table using data from " + key)

            connection.autocommit = False
            cursor.execute(
                f"""
                DELETE FROM {table} USING tmp t2
                WHERE {table}.{table.currency_code} = t2.{table.currency_code}
                AND {table}.{table.base_currency_code} = t2.{table.base_currency_code}
                AND {table}.{table.exchange_rate_date} = t2.{table.exchange_rate_date}
                """
            )
            logging.info(
                f"Deleted duplicate entries from {table}.\n{str(cursor.rowcount)} row(s) affected.")

            cursor.execute(
                "INSERT INTO {0} ({1}) SELECT {1} FROM tmp;".format(
                    table, temp))
            connection.commit()
            logging.info(
                f"Inserted data into {str(table)}.\n{str(cursor.rowcount)} row(s) affected.")

            connection.autocommit = True
            cursor.execute("DROP TABLE tmp")
            logging.info("Dropped temp table")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2010, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG(
    dag_id='exchange_rates',
    default_args=default_args,
    description='A DAG that daily extracts the exchange rates with base currency USD',
    schedule_interval="1 16 * * *",
    max_active_runs=1)

csv_op = PythonOperator(
    task_id='to_csv',
    provide_context=True,
    python_callable=csv_to_s3,
    dag=dag
)

redshift_op = PythonOperator(
    task_id='to_redshift',
    provide_context=True,
    python_callable=s3_to_redshift,
    dag=dag
)

post_update = BashOperator(
    task_id="post_update",
    dag=dag,
    bash_command=""
)

csv_op >> redshift_op >> post_update >> vacuum_and_analyze
