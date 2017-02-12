import pickle
import psycopg2
import pandas as pd 
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


    return con

if __name__ == "__main__":
    query = """SELECT * FROM node_communities WHERE "Community" = 0;"""
    db_q = pd.read_sql_query(query, con)
    nodes = list(db_q['Node'].values)
    nodes = ['node' + str(n) for n in nodes]
    with open('network_broad.pickle', 'rb') as f:
        network = pickle.load(f)

    subgraph = network.subgraph(nodes)

layout = subgraph.layout('kk')
visual_style = {}
visual_style['vertex_size'] = 10
visual_style['vertex_color'] = 'red'
visual_style['layout'] = layout

plot(subgraph, '/home/msweeten/insight/app/static/images/community0.png', **visual_style)
