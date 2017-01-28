from igraph import *
from collections import Counter
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import pickle

def load_network_db():
    """Opens network file from SQL database
    """
    cfg = open('Config.cfg').read()
    split = cfg.split('\n')
    dbname = 'classic_db'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host = 'localhost', password = pswd)
    query = 'SELECT * FROM network;'
    db = pd.read_sql_query(query, con)
    query = 'SELECT * FROM node_data;'
    db_genre = pd.read_sql_query(query, con)

    
    genres = list(set(db_genre['Genre'].values))
    engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))
    return(db, genres, con, engine)
def create_network(db):
    """Creates a network using Networkx
    """
    nodes1 = list(db['Node A'].values)
    nodes2 = list(db['Node B'].values)
    nodes = list(set(nodes1 + nodes2))
    network = Graph()
    for n in nodes:
        print 'Node ' + str(n) + ' out of ' + str(len(nodes))
        network.add_vertex('node' + str(n))

    subset = db[['Node A', 'Node B']]
    for i in range(len(subset)):
        edge = list(subset.iloc[i].values)
        print edge
        network.add_edge('node' + str(edge[0]), 'node' + str(edge[1]))
    #pickle network
    with open('network.pickle', 'rb') as f:
        pickle.dump(network, f)
    return network

def describe_communities(network, con):
    """Runs Infomap through iGraph
    Weights Vertices according to number of songs in node
    Identifies communities and labels them according
    to frequent album words
    
    Expand to multiple models for validation
    """
    query = 'SELECT * FROM classical_song_nodes;'
    verts = pd.read_sql_query(query, con)
    verts = list(verts['node'].values)
    vert_set = list(set(verts))
    weights = []
    count = Counter(verts)
    for v in vert_set:
        node_length = count[v]
        weights.append(node_length)
    info_map = network.community_infomap()
    info_map50 = network.community_infomap(trials = 50)
    with open('communities.pickle', 'rb') as f:
        pickle.dump(infomap50, f)
    return(info_map, info_map50)
            
    

def assign_labels(model, genres, con, engine):
    """Assigns labels to communities via 
    Lists the top 3 weighted labels (based on training set) 
    For each community

    Places data in SQL database

    Model is an iterable datatype
    """
    query = 'SELECT * FROM node_data WHERE Known = 1;'
    db_nodes = pd.read_sql_query(query, con)
    music_results = pd.DataFrame(columns = ('Node', 'Community', 'Label1', 'Label2', 'Label3'))
    labels = []
    node_iter = 0
    for nodes in model:
        node_list = list(nodes)

        node_genres = db_nodes['Genre'][db_nodes['Node'].isin(node_list)]
        node_genres = node_genres.values
        counts = Counter(node_genres)
        most_common = counts.most_common(3)
        labels = ['']*3
        for m in range(len(most_common)):
            curr_label = list(m)[0]
            labels[m] = curr_label
        for n in node_list:
            node_data = [n, node_iter] + labels
            music_results.append(node_data)
        node_iter += 1
    music_results.to_sql('communities', engine, if_exists = 'replace')
if __name__ == '__main__':
    print('Starting...')
    dataset, genres, con, engine = load_network_db()
    print('Creating Graph Object')
    net = create_network(dataset)
    print('Running Model')
    model, model50 = describe_communities(net, con)
    with open('Example.txt', 'w') as txt:
        txt.write(str(model))
    with open('Example50.txt', 'w') as txt:
        txt.write(str(model50))
    layout = net.layout('kk')
    plot(net, "song_network.pdf", layout)


    #print('Assign Labels')
    #assign_labels(model, genres, con, engine)
    #print('Finished Labels')
    #communities_org(con, engine)
    #draw_network(net)
