import networkx as nx
from collections import Counter

def load_network_db():
    """Opens network file from SQL database
    """
    cfg = open('Config.cfg').read()
    split = cfg.slpit('\n')
    dbname = 'classical_db'
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
    model = nx.algorithms.communities.asyn_lpa_communities(network)
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
    labels = []
    for nodes in model:
        node_genres = db_nodes['Genre'][db_nodes['Node'].isin(nodes)]
        node_genres = node_genres.values
        counts = Counter(node_genres)
        most_common = counts.most_common(3)
        labels.append(most_common)
    
    return(labels)
        
        
        
        
                                        
    
if __name__ == '__main__':
    dataset, genres, con, engine = load_network_db()
    net = create_network(dataset)
    model = describe_communities(db, net)
    assign_labels(mod, genres, con, engine)
    

    
    
