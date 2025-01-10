import pandas as pd
import sys
add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)
from init import *

# wrapper function:
def etl(a1, p1, p2, ti):
    e_rtn_obj = extract_fn()
    t_rtn_obj = transform_fn(a1, e_rtn_obj)
    l_rtn_obj = load_fn(p1, p2, e_rtn_obj)
    return None

def extract_fn():
    print("Logic to extract data!")
    print("Value of Global variable is: ", TITLE)
    # rtn_val = "Hello Nalla!"
    # return rtn_val
    
# In this section we passing the xcom vasue as simple string datatypes/values these are son serializable,
# but for Dataframe like return values
    # we need some workaround, lets see in next dag files.... it require pickles(its unwanted overhead..so another approach here)
    
    # #Creating Dataframe
    details = {
    'cust_id' : [1, 2, 3, 4],
    'Name' : ['Nalla', 'Raju', 'Anu', 'Mega']
    }
    df = pd.DataFrame(details)
    return df


# XCOM : Cross-Communication
# ti stands for Task Instance (Each execution of DAG creates its own task instance)
    # By using the ti, Xcom pull function from which fn you are trying to get the value - i.e:  Here its from extract_fn()
def transform_fn(a1, e_rtn_df):
    extract_rtn_obj = e_rtn_df
    print(f"The value of extract_rtn_obj is {extract_rtn_obj}")
    
    print("The value of a1 is :", a1)
    print("Logic to Transform Data")
    return 10

def load_fn(p1, p2, e_rtn_df):
    extract_rtn_obj = e_rtn_df
    print("type of extract_rtn_obj is {}".format(type(extract_rtn_obj)))
    print(f"The value of extract_rtn_obj is : {extract_rtn_obj}")
    
    print("The value of p1 is :", p1)
    print("The value of p2 is :", p2)
    print("Logic to Load Data!")