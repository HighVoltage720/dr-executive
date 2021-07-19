# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 14:09:42 2021

@author: KapoSi01
"""
#
#import os 
#
#print(os.system("pwd"))


#import subprocess
#
#useless_cat_call = subprocess.Popen(["cat"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
#output, errors = useless_cat_call.communicate(input="Hello from the other side!")
#useless_cat_call.wait()
#print(output)
#print(errors)


#import subprocess
##
##useless_cat_call = subprocess.run(["cat"], stdout=subprocess.PIPE, text=True, input="Hello from the other side")
##print(useless_cat_call.stdout) 
#
#
#list_files = subprocess.run(["ls", "-l"])
#print(list_files)

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG

import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import yaml
import time
import regex as re

from pytz import timezone
tz = timezone('EST')

default_args = {
    'owner': 'QE/Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 26)
}


#file_path = "C:\\Users\\KapoSi01\\Documents\\Projects\\Untitled Folder\\DNS_logs_status_update\\"
file_path = "/home/Performance_System_analysis/"



with open(file_path + 'config_dr.yaml') as stream:
#with open('/home/Performance_System_analysis/config_dr.yaml') as stream:
    config_details = yaml.load(stream)
    url = config_details["uptrends"]["url"]
    username = config_details["uptrends"]["username"]
    password = config_details["uptrends"]["password"]
    authentication = (username, password)
    mysql_id = config_details["sqldb"]["username"]
    mysql_password = config_details["sqldb"]["password"]
    mysql_server = config_details["sqldb"]["server"]
    mysql_database = config_details["sqldb"]["database"]
    mysql_port = config_details["sqldb"]["port"]




def get_status(application_url,production_ip,dr_ip):
    # try:
    time.sleep(5)
    stream = os.popen('nslookup ' + application_url)
    print(application_url)
    output = stream.read()
    print(output)
    ls = output.split("Address: ")
    ip = ls[-1].replace("\n","").strip()
    envt = ""
    if ip == str(production_ip):
        envt = "Primary"
    elif ip == str(dr_ip) :
        envt = "DR"
    else:
        envt = "other"
        time.sleep(5)
        stream = os.popen('nslookup ' + application_url)
        output = stream.read()
        ls = output.split("Address: ")
        ip = ls[-1].replace("\n","").strip()
        if ip == str(production_ip):
            envt = "Primary"
        elif ip == str(dr_ip) :
            envt = "DR"
        else:
            ips = re.findall("Address:\s*(\S+)",output)
            ip = ips[-1]
            if ip == str(production_ip):
                envt = "Primary"
            elif ip == str(dr_ip) :
                envt = "DR"
            else:
                envt = "other"

    print(application_url,ip,production_ip, envt)
    return envt
    # except Exception as e:
    #     print("error in get_status at ",e)



def main_status(ds, **kwargs):
    try:
        df = pd.read_csv(file_path + "IPLookup.csv")
        df["Envt_current"]  = df.apply(lambda x: get_status(x.Application,x.Primary1,x.DR), axis =1)
        df["Time"] = datetime.now()
        conn_string = "mysql+pymysql://" + mysql_id + ":" + mysql_password + "@" + mysql_server+ ":" + mysql_port + "/" + mysql_database
        engine = create_engine(conn_string)
        conn = engine.connect()
        df.to_sql("DR_application_status", conn, if_exists="append", index=False, chunksize=171, method="multi")
        print("done")
        conn.close()
        engine.dispose()
    except Exception as e:
        print("exception as ", e)
        
        
dag = DAG(dag_id='DR_application_status', default_args=default_args,schedule_interval='20 * * * *')

start_get_uptrends = PythonOperator(
    task_id='t_DR_application_status', trigger_rule = 'all_done',
    provide_context=True,
    python_callable=main_status,
    dag=dag)

start_task = DummyOperator(task_id = 'start',  trigger_rule = 'all_done', dag = dag)
end_task = DummyOperator(task_id = 'end',  trigger_rule = 'all_done', dag = dag)

start_task.set_downstream(start_get_uptrends)
start_get_uptrends.set_downstream(end_task)






