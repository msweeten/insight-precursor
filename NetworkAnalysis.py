import netkit
import arff

def load_network_db():
    """Opens network file from SQL database
    """
    cfg = open('Config.cfg').read()
    split = cfg.slpit('\n')
    dbname = 'spot_db'
    username = split[2]
    pswd = split[3]
    con = None
    con = psycopg2.connect(database = dbname, user = username, host = 'localhost', password = pswd)
    query = 'SELECT * FROM node_data;'
    db = pd.read_sql_query(query, con)
    genres = list(set(db['genre'].values))
    grab_network_sql = 'SELECT * FROM network;'
    db2 = pd.read_sql_query(query, con)
    return(db, db2, genres)

def write_netkit_files(db, db2, genres, validate = False):
    """Writes files for NetKit inference
    1. .arff file for network structure
    2. .rn file for connections
    3. .csv files for known and unknown labels
    """
    arff = 'song_schema.arff'
    rn = 'songs.rn'
    known_csv = 'songs_known.csv'
    unknown_csv = 'songs_unknown.csv'

    with open(arff, 'w') as fp:
        fp.write('@nodetype Song

@attribute Name KEY')
        
        fp.write('@attribute Genre {' + ', '.join(genres) + '}')
        fp.write('@nodedata ' + csv)
        fp.write('')
        fp.write('@edgetype Linked Song Song')
        fp.write('@Reversible')
        fp.write('@edgedata ' + rn)
    with open(rn, 'w') as fp:
        for i in range(len(db2)):
            edge = list(db2.iloc[i])
            edge = ','.join(edge)
            fp.write(edge)
    if validate:
        pass
    if not validate:
        known_sub = db[['Node', 'Genre']][db['Known'] == 1]
        all_initial = db[['Node', 'Genre']]
        known_sub.to_csv('known.csv', columns = None, index = False)
        all_initial.to_csv('all_initial.csv', columns = None, index = False)

    
if __name__ == '__main__':
    node_data, dataset, genres = load_network_db()
    write_netkit_files(node_data, dataset, genres)
    netkit.run()
    
    
