import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import itertools

def load_data_sql():
    """Loads data from SQL
    """
    cfg = open('Config.cfg').read()
    split = cfg.split('\n')
    dbname = 'classical_db'
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

        song_data.to_sql('classical_song_nodes', engine, if_exists = 'replace')
            
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
    network = pd.DataFrame(columns = ('Node A', 'Node B', 'Weight'))
    query = 'SELECT * FROM classical_song_nodes;'
    dataset = pd.read_sql_query(query, con)
    albums = list(set(dataset['album_uri'].values))
    for a in albums:
        album_sub = dataset[dataset['album_uri'] == a]
        nodes = list(set(album_sub['node'].values))
        nodes.sort()
        edges = list(itertools.combinations(nodes, 2))
        for e in edges:
            edge_list = list(e) + [1]
            network.append(edge_list, ignore_index = True)

    network['Node A'] = 'Node' + network['Node A'].astype(str)
    network['Node B'] = 'Node' + network['Node B'].astype(str)    
    network.to_sql('network', engine, if_exists='replace')

def network_metadata(con, engine):
    """Takes node data and assigns a label to training data
    """
    query = 'SELECT * FROM classical_song_nodes;'
    node_data = pd.read_sql_query(query, con)
    print('Node List')
    nodes = list(set(node_data['node']))
    node_set = pd.DataFrame(columns = ('Node', 'Known', 'Genre'))
    print('Starting...')
    for i in range(len(nodes)):
        n = nodes[i]
        print('Node ' + str(i) + ' of ' + str(len(nodes)) + ' nodes')
        subset = node_data[node_data['node'] == n]
        if any(subset['set_type'] == 'training'):
            training_vote = subset[subset['set_type'] == 'training']
            #could have multiple modes
            known = 1
        else:
            training_vote = subset[subset['set_type']]
            known = 0
        mode = training_vote['genre'].mode()
        if len(mode) > 1:
            mode = mode[0]
        node_set.append([n, known, mode])
    node.to_sql('node_data', engine, if_exists='replace')
    
if __name__ == '__main__':
    con, engine = load_data_sql()
    print('Start Matches')
    #match_songs(con, engine)
    print('Create Network DB')
    create_network(con,engine)
    network_metadata(con,engine)

