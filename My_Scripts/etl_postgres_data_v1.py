import pandas as pd
from datetime import datetime as dtime

import sys
add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)

from init import * 
from postgres_conn_v1 import *

def etl():
    # conn_obj = get_db_conn()
    conn_obj = get_pg_hook_conn()
    e_df = extract_fn(conn_obj)
    t_df = transform_fn(e_df)
    l_rtn_obj = load_fn(t_df, conn_obj)
    
    return None

def extract_fn(conn_obj):
    select_qry = "select * from employees"
  #  df = pd.read_sql_query(select_qry,conn_obj) # syntax with sqlAlchemy
    df = conn_obj.get_pandas_df(select_qry) #syntax with pg Hook
    print(df)
    return df

def transform_fn(df):
    df["Test_airflow_col"] = dtime.now()
    return df

def load_fn(df, conn_obj):
    
    ##syntax with Hook
    col = list(df.columns)
    rows = list(df.itertuples(index=False, name=None))
    conn_obj.insert_rows(table="employees", rows=rows,target_fields=col)
    
    cursor = conn_obj.get_conn().cursor()
    cursor.execute("call sp_update_source_audit();")
    return 1
    