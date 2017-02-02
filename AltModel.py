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
    dbname = 'classic_limited'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host = 'localhost', password = pswd)
    query = 'SELECT * FROM network_broad;'
    db = pd.read_sql_query(query, con)
    query = 'SELECT * FROM node_data_broad;'
    db_genre = pd.read_sql_query(query, con)
    
    genres = list(set(db_genre['Genre'].values))
    engine = create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))
    return db, genres, con, engine
def create_network(db):
    """Creates a network using Networkx
    """
    nodes1 = list(db['NodeA'].values)
    nodes2 = list(db['NodeB'].values)
    nodes = list(set(nodes1 + nodes2))
    network = Graph()
    for n in nodes:
        print 'Node ' + str(n) + ' out of ' + str(len(nodes))
        network.add_vertex('node' + str(n))

    edges = []
    subset = db[['NodeA', 'NodeB']]
    for i in range(len(subset)):
        edge = list(subset.iloc[i].values)
        print 'Edge ' + str(i) + ' out of ' + str(len(subset) - 1)
        edges.append(('node' + str(edge[0]), 'node' + str(edge[1])))
        #network.add_edge('node' + str(edge[0]), 'node' + str(edge[1]))
    network.add_edges(edges)
    #pickle network
    with open('network_broad.pickle', 'wb') as f:
        pickle.dump(network, f)
    return network

def describe_communities(network, con):
    """Runs Infomap through iGraph
    Weights Vertices according to number of songs in node
    Identifies communities and labels them according
    to frequent album words
    
    Expand to multiple models for validation
    """
    ew_query = 'SELECT * FROM network_broad;'
    weights = pd.read_sql_query(ew_query, con)
    weights = list(weights['Weights'].values)
    print str(len(weights))
    #info_map = network.community_infomap()
    info_map = network.community_infomap(edge_weights = weights)
    #info_map50 = network.community_infomap(weights, trials = 50)
    with open('communities_broad.pickle', 'wb') as f:
        pickle.dump(info_map, f)
    return info_map
            
if __name__ == '__main__':
    print('Starting...')
    dataset, genres, con, engine = load_network_db()
    print('Creating Graph Object')
    if os.path.isfile('network_broad.pickle'):
        with open('network_broad.pickle', 'rb') as f:
            net= pickle.load(f)
    else:
        net = create_network(dataset)
    print('Running Model')
    model= describe_communities(net, con)
    
    with open('Example_broad.txt', 'w') as txt:
        txt.write(str(model))

    #print('Assign Labels')
    #assign_labels(model, genres, con, engine)
    #print('Finished Labels')
    #communities_org(con, engine)
    #draw_network(net)
