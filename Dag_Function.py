# this is python file to help me for make data cleaning and transformation
import pandas as pd

def clean(df):
    #Drop the "Province/State","Lat" & "Long" columns
    #Group by "Country/Region" and caculate the sum value
    #Reset the index of the dataframe
    clean_df = df.drop(columns=['Province/State','Lat','Long']).\
                    groupby('Country/Region').sum().\
                    reset_index()
    return clean_df

def convert(clean_df):
    #Convert columns of dates into a new column 'Date', and store the data in a new column 'Number'
    converted = pd.melt(clean_df, id_vars=["Country/Region"], 
                  var_name="Date", value_name="Number")
    #Convert the data type from string to datetime64 for the data in column 'Date' 
    converted['Date'] = pd.to_datetime(converted['Date'])
    #Sort the dataset by "Country/Region" and "Date"
    converted_df = converted.sort_values(by=["Country/Region","Date"])
    
    return converted_df 

def dailychange(convert_df):
    #Change the index in order to apply diff() function
    convert_df = convert_df.set_index(['Country/Region','Date'])
    
    #Create a new column to store the difference between rows
    convert_df['amount_of_increase']=convert_df.diff()
    
    #Change back the index
    convert_df=convert_df.reset_index()
    
    #Run a for loop to check the boundary rows where the 'Country' changes, and change the value of difference to 0
    for i in range(0,int(convert_df.index.size)-1):
        if convert_df['Country/Region'][i] != convert_df['Country/Region'][i+1]:
            convert_df.at[i+1,'amount_of_increase'] = 0
        else:
            pass
    
    #Fill all NaN with 0
    convert_df=convert_df.fillna(0)

    # Convert the column to integer
    convert_df['amount_of_increase'] = convert_df['amount_of_increase'].astype(int)
    
    return convert_df
