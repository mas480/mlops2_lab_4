import requests
import pandas as pd
import re
import warnings
warnings.filterwarnings("ignore")
import time
import datetime     
from dateutil.relativedelta import relativedelta 
import os
import matplotlib.pyplot as plt
import json
import sqlalchemy
import numpy as np
import dill
import pytz
import poplib
from email import parser
import sklearn.linear_model as sklm
import smtplib, ssl
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.application import MIMEApplication
from datetime import datetime, timezone
from openpyxl import load_workbook


def forecast_from_bd_24h(gtp_code):
    
    start_date = (datetime.today()+ pd.to_timedelta('1 day')).strftime('%Y-%m-%d')
    stop_date = (datetime.today()+ pd.to_timedelta('2 day')).strftime('%Y-%m-%d')
    
    eng = sqlalchemy.create_engine('postgresql://postgres:password@192.168.255.111:5432/test')
    
    query_meteodata = f"""
    select period_start, gtp_code, param_name, param_value
    from forecast
    where gtp_code = '{gtp_code}'
    and param_name = 'power'
    and period_start >= '{start_date}' 
    and period_start < '{stop_date}'
    """
    df_query = pd.read_sql_query(query_meteodata,eng)
    df_forecast = df_query.loc[:,['period_start','param_name','param_value']].pivot_table(index= 'period_start',
                                                                                         columns = 'param_name',
                                                                                         values='param_value')
    if gtp_code == 'burnoe_1':
        df_forecast.columns = ['T-1']
    else:
        df_forecast.columns = ['T-2']
        
    return df_forecast


def creating_file():
    df1 = forecast_from_bd_24h('test_1')
    df2 = forecast_from_bd_24h('test_2')
    result = pd.concat([df1, df2], axis=1).round(4)
    start_date = (datetime.today()+ pd.to_timedelta('1 day')).strftime('%Y-%m-%d')
    
    if not os.path.exists('forecast_file'):
        os.makedirs('forecast_file')
    
    result.to_excel(f'forecast_file/forecast_{start_date}.xlsx', sheet_name = f'{start_date}')
    
    wb = load_workbook(f'forecast_file/forecast_{start_date}.xlsx')
    ws = wb.active
    ws.column_dimensions['A'].width = 25
    wb.save(f'forecast_file/forecast_{start_date}.xlsx')


def send_results(mail_serv,login,password,send_from,send_to,port,files,myfilename):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)  
    msg['Subject'] = 'Прогноз'

    msg.attach(MIMEText(''))

    for f in files or []:
        with open(f, "rb") as fil: 
            ext = f.split('.')[-1:]
            attachedfile = MIMEApplication(fil.read(), _subtype = ext)
            attachedfile.add_header(
                'content-disposition', 'attachment', filename=myfilename )
        msg.attach(attachedfile)

    #context = ssl._create_unverified_context()
    
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    connection = smtplib.SMTP(mail_serv, port)
    connection.ehlo()
    connection.starttls(context=context)
    connection.ehlo()
    connection.login(login, password)
    
    
    with smtplib.SMTP(mail_serv, port) as connection:
        connection.ehlo()
        connection.starttls(context=context)
        connection.ehlo()
        connection.login(login, password)
        connection.sendmail(send_from, send_to, msg.as_string())