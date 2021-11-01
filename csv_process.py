# dependencies
import requests
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser
import csv
import sqlalchemy
from sqlalchemy import create_engine


#from google.appengine.api import app_identity


#############################################################################
# Extract / Transform
#############################################################################

'''
def fetchDataToLocal():
    """
    we use the python requests library to fetch the nyc in json format, then
    use the pandas library to easily convert from json to a csv saved in the
    local data directory
    """
    
    # fetching the request
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)

    # convert the response to a pandas dataframe, then save as csv to the data
    # folder in our project directory
    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("date_of_interest")
    
    # for integrity reasons, let's attach the current date to the filename
    df.to_csv("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))
    
'''


#############################################################################
# Load
#############################################################################


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = pg.connect(**params_dic)
    except (Exception, pg.DatabaseError) as error:
        print(error)
        #sys.exit(1) 
    return conn

def read_data():

    csv_file = 'gs://airflow_bucket_faider/user_purchase_sample.csv'
    df = pd.read_csv(csv_file)
    df = df.rename(columns={
        "InvoiceNo": "invoice_number", 
        "StockCode": "stock_code",
        "Description": "detail",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country"
    })
    df = df.astype(object).where(pd.notnull(df), None)
    print(df.head())
    return df


def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()

def sqlLoad(param_dic):
    conn = connect(param_dic)
    df= read_data()
    table="user_purchase"
    execute_many(conn,df,table)