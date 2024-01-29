import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sqlalchemy
from datetime import datetime, timedelta
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.metrics import r2_score

import warnings
warnings.filterwarnings("ignore")

from catboost import CatBoostRegressor
from catboost import Pool


def data_from_bd(gtp_code):  
    eng = sqlalchemy.create_engine('postgresql://name:password@192.168.255.111:5432/test')
    
    query_meteodata = f"""
    select period_start, gtp_code, param_name, param_value
    from history
    where gtp_code = '{gtp_code}'
    """
    df_query = pd.read_sql_query(query_meteodata,eng)
    df_history = df_query.loc[:,['period_start','param_name','param_value']].pivot_table(index= 'period_start',
                                                                                         columns = 'param_name',
                                                                                         values='param_value')
    return df_history


def data_processing(df):
    df = df[df['power'] < 50000]  # непонятные выбросы
    df['power'] = df['power']/1000
    df.index = df.index.astype(str)
    df['index'] = df.index
    df['index'] = pd.to_datetime(df['index'], format = '%Y-%m-%d %H:%M')
    df.index = df['index']
    df = df.drop(['index'], axis=1)
#    df.index = df.index + timedelta(hours=10)
    
    filt_date = ((df.index >= '2021-09-01') & (df.index < '2024-01-01'))
    df = df[filt_date]
    df = df.dropna()
    
    df['hour'] = df.index.hour
    df['day'] = df.index.day
    df['month'] = df.index.month
    
    df = df[['power', 'hour', 'day', 'month',
             'cloudcover', 'cloudcover_high', 'cloudcover_low', 'cloudcover_mid', 
             'diffuse_radiation', 'direct_normal_irradiance', 'direct_radiation',
             'relativehumidity_2m', 'shortwave_radiation', 'temperature_2m',
             'weathercode', 'winddirection_10m', 'windspeed_10m']]
    
    df1 = df.copy()
    df1 = df1.fillna(0)
    
    target_str = list(df.columns)[0]
    weather_list = list(df.columns)[4:]
    
    # for i in range(24):
    #     df1[f'{target_str}_-{i+8}h'] = df1[f'{target_str}'].shift(i+8)
    
    for i in weather_list:
        for j in range(24):
            df1[f'{i}_-{j+1}h'] = df1[f'{i}'].shift(j+1)

    for i in weather_list:
        for j in range(48):    
            df1[f'{i}_+{j+1}h'] = df1[f'{i}'].shift(-j-1)

    df1 = df1.dropna()
    
    # scaler = MinMaxScaler()
    # scaler.fit(df1)
    # scaled = scaler.fit_transform(df1)
    # df1 = pd.DataFrame(scaled, columns=df1.columns, index = df1.index)
    
    X = df1.drop(columns = ["power"])
    y = df1.power
    features_names = list(df1.drop(columns = ["power"]).columns)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42, shuffle=False)
    
    return df1, X_train, X_test, y_train, y_test, features_names



def model_catboost_not_optuna(X_train, X_test, y_train, y_test, features_names, gtp_code):
    train_data = Pool(
        data=X_train, 
        label=y_train,
        feature_names=features_names)
    
    eval_data = Pool(
        data=X_test,
        label=y_test,
        feature_names=features_names)
    
    model = CatBoostRegressor(iterations = 1000,
                              early_stopping_rounds=300,
                              verbose = 100,
                              depth = 7,
                              eval_metric= 'RMSE')
    
    model.fit(X=train_data,
              eval_set=eval_data)
    
    r2_sc = r2_score(y_test, model.predict(eval_data)).round(4)
    
    if not os.path.exists('models'):
        os.makedirs('models')
    
    model.save_model(f"./models/model_catboost_{gtp_code}.bin")
    return r2_sc