import pandas as pd
import sys
add_path_to_sys = "/home/nalla/airflow/My_Scripts"
sys.path.append(add_path_to_sys)
from init import *

def extract_fn():
    print("Logic to extract data!")
    #print("Value of Global variable is: ", TITLE)
    rtn_val = "Hello Nalla!"
    return rtn_val
# In this section we passing the xcom vasue as simple datatypes/values, but for Dataframe like return values
    # we need some workaround, lets see in next dag files.... 
    
# Creating Dataframe
# details = {
#   'cust_id' : [1, 2, 3, 4],
#   'Name' : ['Nalla', 'Raju', 'Anu', 'Mega']
#    }
# df = pd.DataFrame(details)
# return df


# XCOM : Cross-Communication
# ti stands for Task Instance (Each execution of DAG creates its own task instance)
    # By using the ti, Xcom pull function from which fn you are trying to get the value - i.e:  Here its from extract_fn()
def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =['EXTRACT'])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    extract_rtn_obj = xcom_pull_obj[0]
    print(f"The value of xcom pull object is {extract_rtn_obj}")
    
    print("The value of a1 is :", a1)
    print("Logic to Transform Data")
    return 10

def load_fn(p1, p2, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids =['EXTRACT'])
    print("type of xcom pull object is {}".format(type(xcom_pull_obj)))
    print(f"The value of xcom pull object is {xcom_pull_obj[0]}")
    
    print("The value of p1 is :", p1)
    print("The value of p2 is :", p2)
    print("Logic to Load Data!")