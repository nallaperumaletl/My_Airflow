TITLE = "ETL Developer"

def fn():
    print("Inside function!")
    print("The value of TITLE is :", TITLE)
    #TITLE = "DATA ENGINEER"   
    #we shouldn't provide the local veriable inside the function once provided as a "GLOBAL VARIABLE WITH THE SAME NAME"
    
    rtn_val =  "Hello Nalla Perumal!"
    return rtn_val


rtn_value = fn()
print(f"\nOutside function!")
print("The value of TITLE is :", TITLE)
print("The value of rtn_value is :", rtn_value)