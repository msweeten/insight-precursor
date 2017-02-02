import pickle
from igraph import *
import psycopg2
import numpy as np
import pandas as pd
from collections import Counter
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

def load_data_sql():
    """Loads data from SQL
    """
    cfg = open('Config.cfg').read()
    split = cfg.split('\n')
    dbname = 'classic_limited'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host='localhost', password=pswd)
    engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))    
    return con, engine

def create_db(con, engine):
    """Loads communities from pickle file

    Searches communities to identify majority
    vote genre from labelled data
    """
    with open('communities_broad.pickle', 'rb') as f:
        communities = pickle.load(f)
    with open('network_broad.pickle', 'rb') as f:
        network = pickle.load(f)

    
    query = 'SELECT * FROM node_data_broad;'
    db =  pd.read_sql_query(query, con)
    
    query2 = 'SELECT node, song_uri, popularity FROM classical_song_nodes_b;'
    db_nodes = pd.read_sql_query(query2, con)
    node_list = list(set(list(db_nodes['node'].values)))
    
    #Just Node, Uri, popularity, OldGenre, popularity
    unique_list = []
    for n in node_list:
        print 'On node ' + str(n) + ' of ' + str(len(node_list))
        subset = db_nodes[db_nodes['node'] == n]
        max_popularity = max(list(subset['popularity'].values))
        max_row = subset[subset['popularity'] == max_popularity]
        if len(max_row) > 1:
            max_row = max_row.iloc[0]
        #match node_data with node_data_broad
        broad_data = db[db['Node'] == n]
        if list(broad_data['Known'])[0] == 1:
            set_type = 'training'
        else:
            set_type = 'test'
        uri = max_row['song_uri']            
        if not type(uri) is str:
            uri = uri.tolist()[0]
        pop = max_row['popularity']
        if isinstance(pop, np.int64):
            pop = int(pop)
        else:
            pop = int(pop.tolist()[0])
        br_genre = broad_data.iloc[0]['Genre']
        if not type(br_genre) is str:
            br_genre = br_genre.tolist()[0]
        unique_list.append([n, uri, pop, br_genre, set_type])
    community_db = pd.DataFrame(unique_list, columns = ('Node', 'Uri', 'popularity', 'OldGenre', 'set_type'))
    #Node, Community, NewGenre, OldGenre, Uri, popularity
    community_db['NewGenre'] = ['']*len(community_db)
    community_db['Community'] = [-1]*len(community_db)

    vs = VertexSeq(network)
    #search over communities
    for i in range(len(communities)):
        c = communities[i]
        print 'Community ' + str(i) + ' of ' + str(len(communities) - 1)
        #iterate over node list
        node_names = []
        for node in c:
            node_name = vs[node]['name']
            node_names.append(int(node_name.replace('node', '')))

        index_node = community_db[community_db['Node'].isin(node_names)].index.tolist()
        for row in index_node:
            community_db.set_value(row, 'Community', i)

        new_genre = maj_vote(community_db, i)

        index_comm = community_db[community_db['Community'] == i].index.tolist()
        for row in index_comm:
            community_db.set_value(row, 'NewGenre', new_genre)

    community_db.to_sql('node_communities_plural', engine, if_exists='replace', index = False)


def maj_vote(db, community):
    """Provides the majority vote method
    """
    comm_db = list(db['OldGenre'][(db['Community'] == community) & (db['set_type'] == 'training')])
    len_comm = len(comm_db)
    if len_comm > 0:
        counts = Counter(comm_db)
        most_comm = list(counts.most_common(1)[0])
        ident = most_comm[0]
        ident_count = most_comm[1]
        #if float(ident_count)/float(len_comm) > .5:
        #    genre = ident
        #else:
        #    genre = ''
        genre = ident
    else:
        genre = ''
    return genre
            

if __name__ == '__main__':
    connect, engine = load_data_sql()
    print('Starting db')
    create_db(connect, engine)
