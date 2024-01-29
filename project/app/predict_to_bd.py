import requests
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import datetime

from weather_to_bd import *
from models_learn import *
from prepr_for_predict import *


def predict_to_bd(gtp_code):
    
    df_from_bd_pred = data_from_bd_pred(gtp_code)
    X = data_processing_pred(df_from_bd_pred)
    
    model = CatBoostRegressor()
    model.load_model(f"./models/model_catboost_{gtp_code}.bin")

    X['power']=model.predict(X)
    res_df = X[['power']]
    res_df['power'] = res_df['power'].apply(lambda x: 0 if x < 1 else x)
    
    df1 = pd.melt(res_df, 
                  value_vars = res_df.columns, 
                  ignore_index = False)
    
    df1 = df1.rename({'value':'param_value'}, axis=1)
    df1['gtp_code'] = gtp_code
    df1['period_start'] = df1.index
    df1 = df1[['period_start','gtp_code','param_name','param_value']]
    return df1

def upsert_predict_forecast(df_to_insert):
    df_to_insert['period_start'] = df_to_insert['period_start'].astype(str)
    values =  ','.join([str(i) for i in list(df_to_insert.to_records(index=False))]).replace('"', "'") 
    query_string1 = f""" 
    INSERT INTO forecast(period_start, gtp_code, param_name, param_value)
    VALUES {values}
    ON CONFLICT (period_start,gtp_code,param_name)
    DO  UPDATE SET param_value= excluded.param_value
    """ 
    eng = sqlalchemy.create_engine('postgresql://postgres:password@192.168.255.111:5432/test')
    with eng.connect() as connection:
        result = connection.execute(query_string1)
        result.close()
        print(f'upsert_weather_forecast result.rowcount={result.rowcount}, len(df_to_insert) = {len(df_to_insert)}')
    return result.rowcount == len(df_to_insert)
