import requests
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import datetime


def request_meteo_forecast(gtp_code, latitude, longitude):
    
    # datetime_now = datetime.datetime.now()
    # start_date = datetime_now.strftime('%Y-%m-%d')
    # end_date = (datetime_now + datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    meteo_parameters = ['temperature_2m',
                        'relativehumidity_2m',
                        'cloudcover',
                        'cloudcover_low',
                        'cloudcover_mid',
                        'cloudcover_high',
                        'shortwave_radiation',
                        'direct_radiation',
                        'direct_normal_irradiance',
                        'diffuse_radiation',
                        'precipitation',
                        'snowfall',
                        'weathercode',
                        'windspeed_10m',
                        'winddirection_10m',
                        #'sunrise',
                        #'sunset'
                       ]
    # https://archive-api.open-meteo.com/v1/archive - deep archive 
    # http://api.open-meteo.com/v1/forecast - forecasts and shallow archive
    f = requests.get(f"http://api.open-meteo.com/v1/forecast", params = {'past_days': 2,
                                                                        'latitude': latitude,
                                                                        'longitude': longitude,
                                                                        'hourly': meteo_parameters})
    
    payload = f.json()
    df_meteo = pd.DataFrame(payload['hourly'])
    df_meteo.index = df_meteo['time']
    df_meteo['time'] = pd.to_datetime(df_meteo['time'], format='%Y-%m-%dT%H:%M')
    
    df_meteo_long = df_meteo.melt(id_vars = ['time'])
    df_meteo_long['gtp_code'] = gtp_code
    
    df_meteo_long.columns = ['period_start','param_name','param_value','gtp_code']
    df_meteo_long = df_meteo_long[['period_start','gtp_code','param_name','param_value']]
    return df_meteo_long


def request_meteo_history(gtp_code, start_date, latitude, longitude):
    
    datetime_now = datetime.datetime.now()
    end_date = (datetime_now - datetime.timedelta(days=6)).strftime('%Y-%m-%d') # так как выкладывают раз в неделю (примерно)
    meteo_parameters = ['temperature_2m',
                        'relativehumidity_2m',
                        'cloudcover',
                        'cloudcover_low',
                        'cloudcover_mid',
                        'cloudcover_high',
                        'shortwave_radiation',
                        'direct_radiation',
                        'direct_normal_irradiance',
                        'diffuse_radiation',
                        'precipitation',
                        'snowfall',
                        'weathercode',
                        'windspeed_10m',
                        'winddirection_10m',
                        #'sunrise',
                        #'sunset'
                       ]
    # https://archive-api.open-meteo.com/v1/archive - deep archive 
    # http://api.open-meteo.com/v1/forecast - forecasts and shallow archive
    f = requests.get("https://archive-api.open-meteo.com/v1/archive", params = {'latitude': latitude,
                                                                                'longitude': longitude,
                                                                                'hourly': meteo_parameters,
                                                                                'start_date': start_date,
                                                                                'end_date': end_date})
    
    payload = f.json()
    df_meteo = pd.DataFrame(payload['hourly'])
    df_meteo.index = df_meteo['time']
    df_meteo['time'] = pd.to_datetime(df_meteo['time'], format='%Y-%m-%dT%H:%M')
    
    df_meteo_long = df_meteo.melt(id_vars = ['time'])
    df_meteo_long['gtp_code'] = gtp_code
    
    df_meteo_long.columns = ['period_start','param_name','param_value','gtp_code']
    df_meteo_long = df_meteo_long[['period_start','gtp_code','param_name','param_value']]
    return df_meteo_long


def upsert_weather_forecast(df_to_insert):
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

def upsert_weather_history(df_to_insert):
    df_to_insert['period_start'] = df_to_insert['period_start'].astype(str)
    values =  ','.join([str(i) for i in list(df_to_insert.to_records(index=False))]).replace('"', "'") 
    query_string1 = f""" 
    INSERT INTO history(period_start, gtp_code, param_name, param_value)
    VALUES {values}
    ON CONFLICT (period_start, gtp_code, param_name)
    DO  UPDATE SET param_value= excluded.param_value
    """ 
    eng = sqlalchemy.create_engine('postgresql://postgres:password@192.168.255.111:5432/test')
    with eng.connect() as connection:
        result = connection.execute(query_string1)
        result.close()
        #print(f'upsert_weather_history result.rowcount={result.rowcount}, len(df_to_insert) = {len(df_to_insert)}')
    return result.rowcount == len(df_to_insert)