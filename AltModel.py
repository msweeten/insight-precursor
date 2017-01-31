from igraph import *
from collections import Counter
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import pickle
import os.path

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
    return db, genres, con, engine
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
    edges = []
    edge_weights = []
    for i in range(len(subset)):
        edge = list(subset.iloc[i].values)
        print 'Edge ' + str(i) + ' out of ' + str(len(subset) - 1)
        edges.append(('node' + str(edge[0]), 'node' + str(edge[1])))
        edge_weights.append(edge[2])
        #network.add_edge('node' + str(edge[0]), 'node' + str(edge[1]))
    network.add_edges(edges)
    #pickle network
    with open('networkdb2.pickle', 'wb') as f:
        pickle.dump(network, f)
    with open('edge_weights.pickle', 'wb') as f:
        pickle.dump(edge_weights)
    return network, edge_weights

def describe_communities(network, edge_weights, con):
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
    info_map = network.community_infomap(edge_weights = edge_weights)
    #info_map50 = network.community_infomap(weights, trials = 50)
    with open('communitiesdb2.pickle', 'wb') as f:
        pickle.dump(info_map, f)
    return info_map
            
if __name__ == '__main__':
    print('Starting...')
    dataset, genres, con, engine = load_network_db()
    print('Creating Graph Object')
    if os.path.isfile('networkdb2.pickle'):
        with open('networkdb2.pickle') as f:
            net, edge_weights = pickle.load(f)
    else:
        net, edge_weights = create_network(dataset)
    print('Running Model')
    model= describe_communities(net, edge_weights, con)
    
    with open('Exampledb2.txt', 'w') as txt:
        txt.write(str(model))

    #print('Assign Labels')
    #assign_labels(model, genres, con, engine)
    #print('Finished Labels')
    #communities_org(con, engine)
    #draw_network(net)
