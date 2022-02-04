
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG

from sqlalchemy import create_engine
import json

import time
import uptrends
from uptrends.rest import ApiException
from pprint import pprint
import requests
import pandas as pd
import yaml
from datetime import datetime, timedelta
from datetime import date
from sqlalchemy import text
from pytz import timezone
tz = timezone('EST')

default_args = {
    'owner': 'QE',
    'depends_on_past': False,
<<<<<<< HEAD
    'start_date': datetime(2021, 10, 26)
=======
    'start_date': datetime(2021, 4, 26)
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
}


with open('/home/Performance_System_analysis/config_dr.yaml') as stream:
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
stream.close()

def get_data_status(url,method_url,guid,length,past_days,authentication):
    headers = { "Accept": "application/json"}
    # Execute the request
    baseUrl = url
<<<<<<< HEAD
    response = requests.get(baseUrl + method_url+"/" + guid + "?" + "Take=" + length + "&PresetPeriod=" + past_days, auth=authentication, headers = headers, verify = False)
=======
    response = requests.get(baseUrl + method_url+"/" + guid + "?" + "Take=" + length + "&PresetPeriod=" + past_days, auth=authentication, headers = headers)
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
    # Convert the response into string data, which will contain JSON content
    byteData = response.json()
    return byteData

def get_data(url,method_url,authentication):
    headers = { "Accept": "application/json" }
    # Execute the request
    baseUrl = url
<<<<<<< HEAD
    response = requests.get(baseUrl + method_url, auth=authentication, headers = headers, verify = False)
=======
    response = requests.get(baseUrl + method_url, auth=authentication, headers = headers)
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
    # Convert the response into string data, which will contain JSON content
    byteData = response.json()
    return byteData

def get_monitors_product(MonitorGuid):
    method_url ='//Monitor/' + str(MonitorGuid)
    c = get_data(url,method_url,authentication)
    ls = c["CustomFields"]
    try:
        ApplicationName = [i["Value"]  for i in ls if i["Name"]=="ApplicationName"][0]
        BusinessUnit = [i["Value"]  for i in ls if i["Name"]=="BusinessUnit"][0]
        Owners = [i["Value"]  for i in ls if i["Name"]=="Owners"][0]
        FullName = [i["Value"]  for i in ls if i["Name"]=="FullName"][0]
    except:
        ApplicationName = ""
        BusinessUnit = ""
        Owners = ""
        FullName = ""
    return [ApplicationName,BusinessUnit,Owners,FullName]


def get_checkpoint_region(checkpointids):
    ls = checkpointids["Checkpoints"]
    region = []
    for checkpointid in ls:
        method_url ='/CheckpointRegion/' + str(checkpointid)
        c = get_data(url,method_url,authentication)
        try:
            Name = c["Name"]
            region.append(Name)
        except:
            region.append("not_exist")
    return region

def env_name(FullName):
    ls = ["DR","PRI","VIP","N","3rd"]
    for i in ls:
        if i in FullName.upper():
            return i
    if "BOCA" in FullName.upper():
        return "DR"

def run_data(day_type):
    try:
        method_url = '//Monitor'
        x = get_data(url,method_url,authentication)
        df = pd.DataFrame(x)
        services = ["Connect","FTP","Http","Https","MultiStepApi","SFTP","Transaction","WebserviceHttp","WebserviceHttps"]
        df_main = df[df["MonitorType"].isin(services)]
        columns_useful = ["MonitorType","Name","MonitorGuid","SelectedCheckpoints"]
        df_main = df_main[columns_useful]
        df_main[["ApplicationName","BusinessUnit","Owners","FullName"]] = df_main.apply(lambda x: get_monitors_product(x.MonitorGuid), axis =1, result_type='expand')
        df_main = df_main[df_main["BusinessUnit"] == "Business Services"]
#        df_main["Regions"] = df_main.apply(lambda x:get_checkpoint_region(x.SelectedCheckpoints), axis =1)
        ls = []
        for index,row in df_main.iterrows():
            monitor_type = row[0]
            name = row[1]
            monitorguid = row[2]
            checkpoint = row[3]
            ApplicationName = row[4]
            BusinessUnit = row[5]
            Owners = row[6]
            FullName = row[7]
#            Region = row[6]
            try:
                print(monitor_type,name)
                method_url = "//Statistics/Monitor/"
                z = get_data_status(url,method_url,monitorguid,"200",day_type,authentication)
                lstz = z["Data"]
                lstz1 = [i["Attributes"] for i in lstz]
                dfz = pd.DataFrame(lstz1)
                columns_2 = ["Alerts","Checks","ConfirmedErrors","Downtime","DowntimePercentage","StartDateTime","UnconfirmedErrors"]
                dfz = dfz[columns_2]
                dfz["date"] = dfz.apply(lambda x: str(datetime.strptime(x.StartDateTime,"%Y-%m-%dT%H:%M:%S").date()), axis =1)
                dff = pd.DataFrame(columns = ["Alerts","Checks","ConfirmedErrors","Downtime","UnconfirmedErrors","Monitor_guid","Monitor_name","Monitor_type","date","ApplicationName","BusinessUnit","Owners","FullName"] )
                dff.loc[0] = [sum(dfz["Alerts"]),sum(dfz["Checks"]),sum(dfz["ConfirmedErrors"]),sum(dfz["Downtime"]),sum(dfz["UnconfirmedErrors"]),monitorguid,name,monitor_type,dfz["date"].iloc[0],ApplicationName,BusinessUnit,Owners,FullName]
                ls.append(dff)
            except Exception as e:
                print("error :",e)
                print("error in ",name)
        df_data = pd.concat(ls)
        df_data["error"] = df_data.apply(lambda x: 1 if x.Alerts > 0 else 0, axis =1)
<<<<<<< HEAD
        df_data["environment_name"] = df_data.apply(lambda x: env_name(x.FullName), axis =1)
=======
        df_data["environment_name"] = df_data.apply(lambda x: env_name(x.FullName), axis =1) 
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
        conn_string = "mysql+pymysql://" + mysql_id + ":" + mysql_password + "@" + mysql_server+ ":" + mysql_port + "/" + mysql_database
        engine = create_engine(conn_string)
        conn = engine.connect()
        df_data.to_sql("DR_uptrends_daily", conn, if_exists="append", index=False, chunksize=171, method="multi")
        conn.close()
        engine.dispose()
    except Exception as e:
        print("error")
        print(e)


def run_dag(ds, **kwargs):
    try:
        conn_string = "mysql+pymysql://" + mysql_id + ":" + mysql_password + "@" + mysql_server+ ":" + mysql_port + "/" + mysql_database
        engine = create_engine(conn_string)
        conn = engine.connect()
        today = datetime.now(tz)
        new_date = today - timedelta(days=1)
        today = today.strftime('%Y-%m-%d')
        new_date = new_date.strftime('%Y-%m-%d')
        print(today)
        query_yesterdaydata = "Select * from DR_uptrends_daily where date = '"+ str(new_date) +"';"
        query_todaydata = "Select * from DR_uptrends_daily where date = '"+ str(today) +"';"
        df_today = pd.read_sql(query_todaydata,conn)
        df_yesterday = pd.read_sql(query_yesterdaydata,conn)

<<<<<<< HEAD

=======
        
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
        if df_today.empty and df_yesterday.empty:
            ## add new data
            run_data("CurrentDay")
            run_data("PreviousDay")
            print("both days data updated")
            conn.close()
            engine.dispose()
        elif not df_today.empty and not df_yesterday.empty:
<<<<<<< HEAD
            ## yesterday data is not empty and today data is not empty
=======
            ## yesterday data is not empty and today data is not empty 
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
            ## delete todays data and re fetch today's data
            string = "Delete from DR_uptrends_daily where date = '" + str(today) + "';"
            qry = text(string)
            conn.execute(qry)
            conn.close()
            engine.dispose()
            run_data("CurrentDay")
            print("today data is updated")
        elif  df_today.empty and not df_yesterday.empty:
<<<<<<< HEAD
            ## yesterday data is not empty and today data is empty
=======
            ## yesterday data is not empty and today data is empty 
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
            ## delete yesterday data and re fetch yesterday's data and fetch fresh today's data
            string = "Delete from DR_uptrends_daily where date = '" + str(new_date) + "';"
            qry = text(string)
            conn.execute(qry)
            conn.close()
            engine.dispose()
            run_data("CurrentDay")
            run_data("PreviousDay")
            print("yesterday data is deleted and updated and todays data is updated")
        elif not df_today.empty and df_yesterday.empty:
<<<<<<< HEAD
            ## yesterday data is empty and today data is not empty
=======
            ## yesterday data is empty and today data is not empty 
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
            ## delete today's data and fetch yesterday's data and fetch fresh today's data
            string = "Delete from DR_uptrends_daily where date = '" + str(today) + "';"
            qry = text(string)
            conn.execute(qry)
            conn.close()
            engine.dispose()
            run_data("CurrentDay")
            run_data("PreviousDay")
            print("got yesterday data")
        else:
<<<<<<< HEAD
            print("nothing is updated")
            conn.close()
            engine.dispose()

    except:
        print("error in run_dag")

=======
            print("nothing is updated") 
            conn.close()
            engine.dispose()
            
    except:
        print("error in run_dag")
    
>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099


dag = DAG(dag_id='DR_uptrends_daily_1', default_args=default_args,schedule_interval='20 * * * *')

start_get_uptrends = PythonOperator(
    task_id='t_start_get_uptrends', trigger_rule = 'all_done',
    provide_context=True,
    python_callable=run_dag,
    dag=dag)

start_task = DummyOperator(task_id = 'start',  trigger_rule = 'all_done', dag = dag)
end_task = DummyOperator(task_id = 'end',  trigger_rule = 'all_done', dag = dag)

start_task.set_downstream(start_get_uptrends)
start_get_uptrends.set_downstream(end_task)
<<<<<<< HEAD
=======
        

>>>>>>> 52deabdbc4af81d9ea318ab5ced98ee8b7a2f099
