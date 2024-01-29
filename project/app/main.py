from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import sqlalchemy
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timezone

from weather_to_bd import *
from models_learn import *
from prepr_for_predict import *
from predict_to_bd import *
from send_to_email import *

class Request_params(BaseModel):
    gtp_code: str
    

app = FastAPI()

@app.get("/")
def read_root():
    return "This is a test of forecasting the production"


@app.post("/weather_to_bd/")
async def weather_to_bd_fun(req_params: Request_params):
    df_weather_forecast = request_meteo_forecast(req_params.gtp_code)
    check = upsert_weather_forecast(df_weather_forecast)
    if check == True:
        return 'The recording of the weather forecast in the database was SUCCESSFULLY'
    else:
        return 'The recording of the weather forecast in the database was UNSUCCESSFULLY'
    
    
@app.post("/model_training/")
async def model_training_fun(req_params: Request_params):
    df_from_bd = data_from_bd(req_params.gtp_code)
    df1, X_train, X_test, y_train, y_test, features_names = data_processing(df_from_bd)
    r2_score_ = model_catboost_not_optuna(X_train, X_test, y_train, y_test, features_names, req_params.gtp_code)
    return f'r2_score (model {req_params.gtp_code}): {r2_score_}'

@app.post("/forecast_to_bd/")
async def forecast_to_bd_fun(req_params: Request_params):
    df = predict_to_bd(req_params.gtp_code)
    check = upsert_predict_forecast(df)
    if check == True:
        return 'The recording of the power forecast in the database was SUCCESSFULLY'
    else:
        return 'The recording of the power forecast in the database was UNSUCCESSFULLY'

@app.post("/send_forecast_to_email/")
async def send_forecast_to_email_fun():
    
    creating_file()
    
    start_date = (datetime.today()+ pd.to_timedelta('1 day')).strftime('%Y-%m-%d')
    send_to = ['test@mail.ru']
    files = [f'./forecast_file/forecast_{start_date}.xlsx']
    mail_serv = "mail.service.ru"
    port = 581
    login = "login"
    password = 'password'
    send_from = "send_test@mail.ru.ru"
    myfilename = f'Forecast_{start_date}.xlsx'
    files = [f'./forecast_file/forecast_{start_date}.xlsx']
    
    try:
        send_results(mail_serv,login,password,send_from,send_to,port,files,myfilename)
        return 'The forecast has been sent'
    except:
        return 'The forecast has NOT been sent'