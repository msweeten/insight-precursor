import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import itertools
from collections import Counter

def load_data_sql():
    """Loads data from SQL
    """
    cfg = open('Config.cfg').read()
    split = cfg.split('\n')
    dbname = 'classic_db'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host='localhost', password=pswd)
    engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))    
    return(con, engine)

def match_songs(con, engine):
    """Checks SQL database for song_node variable
       Else initializes song_node variable
    """
    query = 'SELECT * FROM classical_songs;'
    song_data = pd.read_sql_query(query, con)    
    if 'node' not in list(song_data.columns):
        node_label = [0]*len(song_data)
        song_data['node'] = node_label
        node_iter = 2
        song_data.set_value(0, 'node', 1)
        for row in range(1, len(song_data)):
            print('Currently on row ' + str(row) + ' of ' + str(len(song_data) - 1))
            row_data = song_data.iloc[row]
            node_data = row_data['node']
            if node_data == 0:
                """Match to other songs
                """
                artists = row_data['artist_id'].split(', ')
                prev_data = song_data[0:row]
                matches = prev_data['node'][(prev_data['artist_id'].str.contains('|'.join(artists))) & (prev_data['song_name'] == row_data['song_name'])]
                if len(matches) == 1:
                    new_node = matches
                    song_data.set_value(row, 'node', new_node)
                if len(matches) > 1:
                    new_node = matches.values[0]
                    song_data.set_value(row, 'node', new_node)
                if len(matches) == 0:
                    song_data.set_value(row, 'node', node_iter)
                    node_iter += 1

        song_data.to_sql('classical_song_nodes', engine, if_exists = 'replace', index = False)
            
        #if artist_uri match, if song_name match
    elif 'node' in list(data.columns) and any(data['node'].isnull()):
        #Stretch Goal: Read in more songs and expand network
        query = 'SELECT * FROM classical_songs WHERE ;'        
        pass
    else:
        pass

def create_network(con, engine):
    """Create the network and places into SQL table in classical_db
    """
    net = pd.DataFrame(columns = ('Node A', 'Node B', 'Weight'))
    query = 'SELECT * FROM classical_song_nodes;'
    dataset = pd.read_sql_query(query, con)
    albums = list(set(dataset['album_uri'].values))
    network = []
    for a in albums:
        print(str(albums.index(a)) + ' out of ' + str(len(albums) - 1))
        #perhaps link all training data for each subgenre
        album_sub = dataset[dataset['album_uri'] == a]
        nodes = list(set(album_sub['node'].values))
        nodes.sort()
        edges = list(itertools.combinations(nodes, 2))
        for e in edges:
            edge_list = list(e) + [1]
            network.append(edge_list)
    genres = ['Avant-Garde', 'Baroque', 'Chant',
                  'Choral',
                  'Early Music', 'Classical Period',
                  'Minimal', 'Opera',
                  'Orchestral', 'Renaissance',
                  'Romantic']
    for g in genres:
        album_gen = dataset[(dataset['set_type'] == 'training') & (dataset['genre'] == g)]
        nodes = list(set(album_sub['node'].values))
        nodes.sort()
        edges = list(itertools.combinations(nodes, 2))
        for e in edges:
            edge_list = list(e) + [1]
            network.append(edge_list)
    net = pd.DataFrame(network, columns = ('Node A', 'Node B', 'Weight'))            
    net.to_sql('network2', engine, if_exists='replace', index = False)

def network_metadata(con, engine):
    """Takes node data and assigns a label to training data
    """
    query = 'SELECT * FROM classical_song_nodes;'
    node_data = pd.read_sql_query(query, con)
    print('Node List')
    nodes = list(set(node_data['node']))
    node_set = []
    print('Starting...')
    for i in range(len(nodes)):
        n = nodes[i]
        print('Node ' + str(i) + ' of ' + str(len(nodes)) + ' nodes') 
        subset = node_data[node_data['node'] == n]
        if any(subset['set_type'] == 'training'):
            if len(subset) == 1:
                training_vote = subset
            else:
                training_vote = subset[subset['set_type'] == 'training']
            #could have multiple modes
            known = 1
        else:
            training_vote = subset
            known = 0
        if len(training_vote) == 1:
            mode = training_vote['genre'].values[0]
        else:
            mode = Counter(list(training_vote['genre'].values))
            mode = mode.most_common(1)
            mode = list(mode[0])[0]
        node_set.append([n, known, mode])
    nset = pd.DataFrame(node_set, columns = ('Node', 'Known', 'Genre'))        
    nset.to_sql('node_data', engine, if_exists='replace', index = False)
    
if __name__ == '__main__':
    con, engine = load_data_sql()
    print('Start Matches')
    #match_songs(con, engine)
    print('Create Network DB')
    create_network(con,engine)
    network_metadata(con,engine)

