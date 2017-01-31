from flask import render_template
from app import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from .a_model import ModelIt

cfg = open('/home/msweeten/insight-precursor/Config.cfg').read()
split = cfg.split('\n')
dbname = 'classic_limited'
username = split[2]
pswd = split[3]
con = None
con = psycopg2.connect(database = dbname, user = username, host='localhost', password=pswd)
engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))    

#start with this
@app.route('/')
def home():
   return(render_template('home.html'))
@app.route('/About')
def about():
   sql_query = """
               SELECT song_name, artists_name, album_name FROM classical_songs LIMIT 50;
               """
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
   return(render_template('about.html', songs = songs))
@app.route('/Contact')
def contact():
   return(render_template('home.html'))


def start_page():
   return(render_template('home.html'))
@app.route('/Avant-Garde')
def avant():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Avant-Garde.Community, Avant-Garde.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Avant-Garde.Node, ORDER BY classical_song_nodes.popularity DESC;
               """
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
            
   return(render_template('Avant-Garde.html', songs = songs))
@app.route('/Baroque')
def baroque():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Baroque.Community, Baroque.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Baroque.Node, ORDER BY classical_song_nodes.popularity DESC;
               """

   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
            
   return(render_template('Baroque.html', songs = songs))
@app.route('/Chant')
def chant():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Chant.Community, Chant.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Chant.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   

   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
            
   return(render_template('Chant.html', songs = songs))
@app.route('/Choral')
def choral():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Choral.Community, Choral.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Choral.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
   return(render_template('Choral.html', songs = songs))
@app.route('/Early+Music')
def early():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Early_Music.Community, Early_Music.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Early_Music.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
   return(render_template('EarlyMusic.html', songs = songs))
@app.route('/Classical+Period')
def classical():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Classical_Period.Community, Classical_Period.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Classical_Period.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
   return(render_template('ClassicalPeriod.html', songs = songs))

@app.route('/Minimal')
def minimal():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Minimal.Community, Minimal.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Minimal.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))    
   return(render_template('Minimal.html', songs = songs))
@app.route('/Opera')
def opera():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Minimal.Community, Minimal.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Minimal.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))   
   return(render_template('Opera.html', songs = songs))
@app.route('/Orchestral')
def orchestral():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Orchestral.Community, Orchestral.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Orchestral.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))
   return(render_template('Orchestral.html', songs = songs))
@app.route('/Renaissance')
def renaissance():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Renaissance.Community, Renaissance.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Renaissance.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   
   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))   
   return(render_template('Renaissance.html', songs = songs))
@app.route('/Romantic')
def romantic():
   sql_query = """
               SELECT classical_song_nodes.song_name, classical_song_nodes.artists_name, classical_song_nodes.album_name, Romantic.Community, Romantic.Label1 FROM classical_song_nodes ON classical_song_nodes.node = Romantic.Node, ORDER BY classical_song_nodes.popularity DESC;
               """   

   query_results = pd.read_sql_query(sql_query, con)
   songs = []
   for i in range(min(len(query_results), 50)):
      songs.append(dict(index=str(i), song_name=query_results.iloc[i]['song_name'], artists_name=query_results.iloc[i]['artists_name'], album_name=query_results.iloc[i]['album_name']))   
   return(render_template('Romantic.html', songs = songs))
