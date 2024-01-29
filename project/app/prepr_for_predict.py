import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sqlalchemy
from datetime import datetime, timedelta
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import r2_score
from sklearn.metrics import accuracy_score

import warnings
warnings.filterwarnings("ignore")

from catboost import CatBoostRegressor
from catboost import Pool

def data_from_bd_pred(gtp_code):  
    eng = sqlalchemy.create_engine('postgresql://postgres:password@192.168.255.111:5432/test')
    
    query_meteodata = f"""
    select period_start, gtp_code, param_name, param_value
    from forecast
    where gtp_code = '{gtp_code}'
    """
    df_query = pd.read_sql_query(query_meteodata,eng)
    df_forecast = df_query.loc[:,['period_start','param_name','param_value']].pivot_table(index= 'period_start',
                                                                                          columns = 'param_name',
                                                                                          values='param_value')
    return df_forecast


def data_processing_pred(df):
    df.index = df.index.astype(str)
    df['index'] = df.index
    df['index'] = pd.to_datetime(df['index'], format = '%Y-%m-%d %H:%M')
    df.index = df['index']
    df = df.drop(['index'], axis=1)
#    df.index = df.index + timedelta(hours=10)
    
#    filt_date = ((df.index >= '2021-09-01') & (df.index < '2024-01-01'))
#    df = df[filt_date]
    df = df.dropna()
    
    df['hour'] = df.index.hour
    df['day'] = df.index.day
    df['month'] = df.index.month
    
    df = df[['hour', 'day', 'month',
             'cloudcover', 'cloudcover_high', 'cloudcover_low', 'cloudcover_mid', 
             'diffuse_radiation', 'direct_normal_irradiance', 'direct_radiation',
             'relativehumidity_2m', 'shortwave_radiation', 'temperature_2m',
             'weathercode', 'winddirection_10m', 'windspeed_10m']]
    
    df1 = df.copy()
    df1 = df1.fillna(0)
    
    weather_list = list(df.columns)[3:]
    
    # for i in range(24):
    #     df1[f'{target_str}_-{i+8}h'] = df1[f'{target_str}'].shift(i+8)
    
    for i in weather_list:
        for j in range(24):
            df1[f'{i}_-{j+1}h'] = df1[f'{i}'].shift(j+1)

    for i in weather_list:
        for j in range(48):    
            df1[f'{i}_+{j+1}h'] = df1[f'{i}'].shift(-j-1)

    df1 = df1.dropna()
    
    X = df1.copy()
    
    return X