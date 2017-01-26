from flask import render_template
from flask import request
from app import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from .a_model import ModelIt

cfg = open('/home/msweeten/insight-precursor/Config.cfg').read()
split = cfg.split('\n')
dbname = 'birth_db'
username = split[2]
pswd = split[3]
con = None
con = psycopg2.connect(database = dbname, user = username, host='localhost', password=pswd)
engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))    

#start with this
@app.route('/')
def start_page():
   
   return(render_template('home.html'))


@app.route('/db_fancy')
def cesareans_page_fancy():
    sql_query = """
               SELECT index, attendant, birth_month FROM birth_data_table WHERE delivery_method='Cesarean';
                """
    query_results=pd.read_sql_query(sql_query,con)
    births = []
    for i in range(0,query_results.shape[0]):
        births.append(dict(index=query_results.iloc[i]['index'], attendant=query_results.iloc[i]['attendant'], birth_month=query_results.iloc[i]['birth_month']))
    return render_template('starter.html',births=births)

@app.route('/input')
def cesareans_input():
    return render_template("input.html")
@app.route('/output')
def cesareans_output():
   #pull 'birth_month' from input field and store it
   patient = request.args.get('birth_month')
   #just select the Cesareans  from the birth dtabase for the month that the user inputs
   query = "SELECT index, attendant, birth_month FROM birth_data_table WHERE delivery_method='Cesarean' AND birth_month='%s'" % patient
   print(query)
   query_results=pd.read_sql_query(query,con)
   print(query_results)
   births = []
   for i in range(0,query_results.shape[0]):
      births.append(dict(index=query_results.iloc[i]['index'], attendant=query_results.iloc[i]['attendant'], birth_month=query_results.iloc[i]['birth_month']))

   the_result = ModelIt(patient,births)
   return(render_template("output.html", births = births, the_result = the_result))


