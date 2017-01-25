import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

def load_data_sql():
    """Loads data from SQL
    """
    cfg = open('Config.cfg').read()
    split = cfg.split('\n')
    dbname = 'spot_db'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host='localhost', password=pswd)
    return(con)

def match_songs(con):
    """Checks SQL database for song_node variable
       Else initializes song_node variable
    """
    query = 'SELECT * FROM song_data_table;'
    song_data = pd.read_sql_query(query, con)    
    if 'node' not in list(song_data.columns):
        node_label = [0]*len(song_data)
        song_data['node'] = node_label
        node_iter = 2
        song_data.iloc[0]['node'] = 1
        for row in range(1, len(song_data)):
            row_data = song_data.iloc[row]
            node_data = row_data['node']
            if node_data == 0:
                """Match to other songs
                """
                artists = node_data['artist_uri'].split(', ')
                prev_data = song_data[0:row]
                matches = prev_data['node'][(prev_data['artist_uri'].str.contains('|'.join(artists))) & (prev_data['song_name'] == row_data['song_name'])]
                if len(matches) > 0:
                    new_node = matches[0]
                    song_data.iloc[row]['node'] = new_node
                if len(matches) == 0:
                    song_data.iloc[row]['node'] = node_iter
                    node_iter += 1

        cfg = open('Config.cfg').read()
        split = cfg.split('\n')
        dbname = 'spot_db'
        username = split[2]
        pswd = split[3]
        engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))
        song_data.to_sql('song_data_table_nodes', engine, if_exists, 'replace')
            
        #if artist_uri match, if song_name match
    elif 'node' in list(data.columns) and any(data['node'].isnull()):
        query = 'SELECT * FROM song_data_table WHERE ;'        
        pass
    else:
        pass

def create_network():
    """Implements a network for semi-supervised learning
    """
    pass

if __name__ == '__main__':
    con = load_data_sql()
    match_songs(con)

