TITLE = "Data Engineer!"

def fn():
    print("Inside function!")
    print("The value of TITLE is :", TITLE)
    #TITLE = "ETL Developer"   
    #we shouldn't provide the local veriable inside the function once provided as a "GLOBAL VARIABLE WITH THE SAME NAME"
    
    rtn_val =  "Hello Nalla Perumal!"
    return rtn_val


rtn_value = fn()
print(f"\nOutside function!")
print("The value of TITLE is :", TITLE)
print("The value of rtn_value is :", rtn_value)