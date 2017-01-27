import networkx as nx
from collections import Counter
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

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
    network = nx.Graph()
    nodes1 = list(db['Node A'].values)
    nodes2 = list(db['Node B'].values)
    nodes = list(set(nodes1 + nodes2))
    network.add_nodes_from(nodes)
    subset = db[['Node A', 'Node B']]
    edges = [tuple(x) for x in subset.values]
    network.add_edges_from(edges)
    return(network)

def describe_communities(network):
    """Runs Label Propogation Algorithm
    Identifies communities and labels them according
    to frequent album words
    
    Expand to multiple models for validation
    """
    model = nx.asyn_lpa_communities(network)
    return(model)

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
    
def communities_org(con, engine):
    """Returns biggest groups for each already defined sub-genre
    """
    genres = ['Avant-Garde', 'Baroque', 'Chant',
                  'Choral',
                  'Early Music', 'Classical Period',
                  'Minimal', 'Opera',
                  'Orchestral', 'Renaissance',
                  'Romantic']
    for g in genres:
        query = "SELECT * FROM communities WHERE Label1 = '%s';" % g
        print(query)
        query_results = pd.read_sql_query(query, con)
        mode_values = Counter(list(query_results['communities'].values))
        mode_values = mode_values.most_common(1)
        mode_values = list(mode_values[0])[0]
        mode = query_results[query_results['communities'] == mode_values]
        db_title = g.replace(" ", "_")
        mode.to_sql(db_title, engine, if_exists = 'replace', index = False)
def draw_network(network):
    nx.draw(network, pos=nx.spring_layout(network))
    import matplotlib.pyplot as plt
    plt.axis('off')
    plt.savefig('/home/msweeten/Dropbox/network.pdf')
    
    
    

if __name__ == '__main__':
    print('Starting...')
    dataset, genres, con, engine = load_network_db()
    print('Creating Graph Object')
    net = create_network(dataset)
    print('Running Model')
    model = describe_communities(net)
    print('Assign Labels')
    assign_labels(model, genres, con, engine)
    print('Finished Labels')
    communities_org(con, engine)
    draw_network(net)
    
