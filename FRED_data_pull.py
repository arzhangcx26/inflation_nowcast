import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

# Create function that outputs API parameters to easily request different queries
def api_params(data_id: str, frequency: str, api_key: str):
    # Set the parameters for the data series that you want to retrieve
    return {
        'file_type': 'json',
        'series_id': data_id,
        'frequency': frequency,
        'api_key': api_key,
    }

def end_of_month(dt, frequency):
    if frequency != 'w':
        if frequency == 'm':
            skip = 1
        elif frequency == 'q':
            skip = 3 
        elif frequency == 'a':
            skip = 12
        else:
            skip = 0
        # Add one month to the original datetime and set the day to 1 to get the first day of the next month
        first_day_of_next_month = dt.replace(day=1) + relativedelta(months=skip)
        # Subtract one day from the first day of the next month to get the last day of the original month
        end_of_month_dt = first_day_of_next_month - relativedelta(days=1)
        
        return end_of_month_dt
    else:
        return dt

def trim_data(df, start_date = '0000-01-01', end_date = '9999-01-01'):
    df = df[df.index <= end_date]
    df = df[df.index >= start_date]
    return df

def pull_data(id_list, frequency, api_key):
    # Set the base URL for the FRED API
    base_url = 'https://api.stlouisfed.org/fred/series/observations?'
    
    # Initialize dataframe
    end_date = '9999-01-01'
    econometric_df = pd.DataFrame()
    first_iteration = True
    
    # Loop through each series id and get data from API
    # Then combine all dataframes into one big dataframe
    for s_id in id_list:
        
        #print(s_id)
        # Send the request to the FRED API to retrieve the data series
        response = requests.get(base_url, params=api_params(s_id, frequency, api_key), verify = False)
    
        # Load the data into a Pandas DataFrame
        datapull = pd.DataFrame(response.json()['observations']).iloc[::-1]
        
        # Drop rows where column 'B' is equal to 5
        datapull = datapull.drop(index=datapull[datapull['value'] == '.'].index).dropna()
        
        # reset indexes for if statement later and drop unnecessary columns
        datapull = datapull.reset_index().drop(columns = ['realtime_start','realtime_end','index'])
        
        # rename value column to series id
        datapull = datapull.rename(columns={'value': s_id})
        
        if datapull.date[0] < end_date:
            #print('Yes')
            end_date = datapull.date[0]
            if first_iteration == False:
                econometric_df = econometric_df[econometric_df.iloc[:,0] <= end_date]
                # reset index
                econometric_df = econometric_df.reset_index().drop(columns = ['index'])
        #print(end_date)
            
        #shave off any rows in the data pull from later time periods
        datapull = datapull[datapull.date <= end_date]
        
        # reset indexes for proper concatenation
        datapull = datapull.reset_index().drop(columns = ['index'])
        
        # change value to float
        datapull[s_id] = datapull[s_id].astype(float)
        
        # Concatenate to original dataframe
        econometric_df = pd.concat([econometric_df, datapull], axis=1, join = 'outer').dropna()
        
        # Mark that it is not the first iteration
        if first_iteration == True:
            first_iteration = False

        time.sleep(1)
        
    # Drop duplicate date columns
    econometric_df = econometric_df.T.drop_duplicates().T

    # Convert the 'date' column to a datetime and set it as the index
    econometric_df['date'] = pd.to_datetime(econometric_df['date'])
    econometric_df['date'] = [end_of_month(date, frequency) for date in econometric_df['date']]
    econometric_df.set_index('date', inplace=True)
    
    # Convert the values to floats
    econometric_df = econometric_df.astype(float)
    
    return(econometric_df)