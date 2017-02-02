import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import itertools
from collections import Counter
import re
from fuzzywuzzy import fuzz

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

def match_songs(con, engine):
    """Checks SQL database for song matches
    Performs partial match as well as full match
    """
    query = 'SELECT * FROM classical_songs;'
    song_data = pd.read_sql_query(query, con)
    done_rows = []
    node = 1
    node_label = [0]*len(song_data)
    song_data['node'] = node_label
    song_data['index'] = list(range(0, len(song_data)))
    for row in range(0, len(song_data)):
        print('Currently on row ' + str(row) + ' of ' + str(len(song_data) - 1))
        row_data = song_data.iloc[row]        
        if row_data['node'] != 0:
            continue
        song_data.set_value(row, 'node', node)
        composer_uri = row_data['artist_id']
        composer_uri = composer_uri.split(', ')[0]
        song_name = row_data['song_name']
        song_key = re.findall('[A-Z] Major|[A-Z] Minor', song_name)
        song_split = re.split('[?:.,]', song_name)
        song_end = song_split[len(song_split) - 1]
        if len(song_key) == 0:
            song_key = ''
        else:
            song_key = song_key[0]

        song_no = re.findall('No\. [0-9]{1,}', song_name)
        if len(song_no) == 0:
            song_no = ''
        else:
            song_no = song_no[0]
        opus = re.findall('Op\. [0-9]{1,}', song_name)
        if len(opus) == 0:
            opus = ''
        else:
            opus = opus[0].upper()

        artist_match = song_data[song_data['artist_id'].str.contains(composer_uri)]
        artist_match = artist_match[artist_match['node'] == 0]
        for i in range(len(artist_match)):
            a = artist_match.iloc[i]
            match_song = a['song_name']
            index = a['index']
            match_key = re.findall('[A-Z] Major|[A-Z] Minor', match_song)
            if len(match_key) == 0:
                match_key = ''
            else:
                match_key = match_key[0]
            match_opus = re.findall('Op\. [0-9]{1,}|WoO [0-9]{1,}|WOO [0-9]{1,}|WWV [0-9]{1,}', match_song)
            if len(match_opus) == 0:
                match_opus = ''
            else:
                match_opus = match_opus[0].upper()
            match_split = re.split('[?:.,]', match_song)
            match_end = match_split[len(match_split)- 1]
            match_no = re.findall('No\. [0-9]{1,}', match_song)
            if len(match_no) == 0:
                match_no = ''
            else:
                match_no = match_no[0]
                
            if match_song in song_split or song_name in match_split:

                song_data.set_value(index, 'node', node)
            elif any(s in match_song for s in song_split):
                """Match full song
                   Match almost full song
                """
                if match_song == song_name:
                    song_data.set_value(index, 'node', node)
                else:
                    fuzzy_match = fuzz.partial_ratio(match_song, song_name)
                    if fuzzy_match > 65:
                        if song_key in match_key or match_key in song_key:
                            if opus == '' or match_opus == '':
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    match_end = match_end.replace(u'ü', 'u')
                                    song_end = song_end.replace(u'ü', 'u')
                                    if match_end == song_end:
                                        song_data.set_value(index, 'node', node)
                            elif opus == match_opus:
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    match_end = match_end.replace(u'ü', 'u')
                                    song_end = song_end.replace(u'ü', 'u')
                                    if match_end == song_end:
                                        song_data.set_value(index, 'node', node)
        print(str(len(song_data[song_data['node'] == node])))
        node += 1
    print(len(list(set(list(song_data['node'].values)))))
    song_data.to_sql('classical_song_nodes_b', engine, if_exists = 'replace', index = False)

def create_network(con, engine):
    """Create the network and places into SQL table in classical_db
    """
    
    net = pd.DataFrame(columns = ('Node A', 'Node B', 'Weight'))
    query = 'SELECT * FROM classical_song_nodes_b;'
    dataset = pd.read_sql_query(query, con)
    albums = list(set(dataset['album_uri'].values))
    network = []
    edge_weights = []    
    for a in albums:
        print(str(albums.index(a)) + ' out of ' + str(len(albums) - 1))
        #perhaps link all training data for each subgenre
        album_sub = dataset[dataset['album_uri'] == a]
        nodes = list(set(album_sub['node'].values))
        nodes.sort()
        edges = list(itertools.combinations(nodes, 2))
        for e in edges:
            edge_list = list(e)
            if edge_list in network:
                e_ind = network.index(edge_list)
                edge_weights[e_ind] += 1
            elif [edge_list[1], edge_list[0]] in network:
                e_ind = network.index([edge_list[1], edge_list[0]])
                edge_weights[e_ind] += 1
            else:
                network.append(edge_list)
                edge_weights.append(1)
    print('Put in SQL')
    cur = con.cursor()
    iter_num = 0
    net = pd.DataFrame(network, columns = ('NodeA', 'NodeB'))
    net['Weights'] = edge_weights
    net.to_sql('network_broad', engine, if_exists='replace', index = False)

def network_metadata(con, engine):
    """Takes node data and assigns a label to training data
    """
    query = 'SELECT * FROM classical_song_nodes_b;'
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
    nset.to_sql('node_data_broad', engine, if_exists='replace', index = False)
    
if __name__ == '__main__':
    con, engine = load_data_sql()
    print('Start Matches')
    #match_songs(con, engine)
    print('Create Network DB')
    create_network(con,engine)
    network_metadata(con,engine)
