#number of flipped genres
#plurality
#majority rule

#diagram for work
#took song, artist, album, genre features
#x = genres
#y = segmented bar plot (stayed the same genre, switched genre to another, was unclassifiable)

import pandas as pd
import numpy as np
import psycopg2
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

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

def community_databases(con):
    """Calls the plurality and Majority Rule data tables
    Creates a new set of classifications from genre switches
    1. Genre Label Stays the Same    
    2. Genre Label Switch (non-blank, test)
    3. Genre Label Switch (non-blank, training)
    4. Genre Label Switch (blank, test)
    
    Only for majority rule:
    5. Genre Label Switch (blank, training)
    """
    query = 'SELECT * FROM node_communities;'
    query2 = 'SELECT * FROM node_communities_plural;'

    maj_rule = pd.read_sql_query(query, con)
    plur_rule = pd.read_sql_query(query2, con)

    return maj_rule, plur_rule
def plot_maj_rule(db):
    """Creates Classification data for maj_rule
    
    Plots Stacked Bar Graph
    """
    db['Value'] = [0]*len(db)
    print(db.columns)
    for i in range(len(db)):
        new_genre = db.iloc[i]['NewGenre']
        old_genre = db.iloc[i]['OldGenre']
        set_type = db.iloc[i]['set_type']
        value = 0
        if new_genre == old_genre:
            if set_type == 'test':
                value = 1
            else:
                value = 2
        else:
            if set_type == 'test':
                value = 3
            else:
                value = 4
            
        db.set_value(i, 'Value', value)
    genres = list(set(list(db['NewGenre'].values)))

    genres = genres[1:]
    genres.sort()
    gb = pd.DataFrame(columns = ('Genre', 'Same_test', 'Same_training', 'Switch_test', 'Switch_train'))
    gb['Genre'] = genres
    print(gb)
    sets = ['test', 'training', 'test',' training']
    for i in range(len(gb)):
        genre = gb.iloc[i]['Genre']
        for v in range(1, 5):
            count = len(db[(db['Value']== v) & (db['NewGenre'] == genre) & (db['set_type'] == sets[v - 1])])
            gb.set_value(i, gb.columns[v], count)
    
    fig, ax1 = plt.subplots(1, figsize = (10, len(genres)))
    
    bar_width = 0.75
    #10th of network

    bar_l = [i+1 for i in range(len(gb['Same_test']))]
    tick_pos = [i+(bar_width/2) for i in bar_l] 
    ax1.bar(bar_l, 
        # using the pre_score data
        gb['Same_test'],
        # set the width
        width=bar_width,
        # with the label pre score
        label='Same Genre (Unlabeled)', 
        # with alpha 0.5
        alpha=0.5, 
        # with color
        color='#F4561D')

    # Create a bar plot, in position bar_1
    ax1.bar(bar_l,
            # using the mid_score data
            gb['Same_training'],
            # set the width
            width=bar_width,

            # with pre_score on the bottom
            bottom=gb['Same_test'],
            # with the label mid score
            label='True Positives',
            # with alpha 0.5
            alpha=0.5,
            # with color
            color='#F1911E')

    # Create a bar plot, in position bar_1
    ax1.bar(bar_l,
            # using the post_score data
            gb['Switch_test'],
            # set the width
            width=bar_width,
            # with pre_score and mid_score on the bottom
            bottom=[i+j for i,j in zip(gb['Same_test'],gb['Same_training'])],
            # with the label post score
            label='New (Unlabeled)',
            # with alpha 0.5
            alpha=0.5,
            # with color
            color='#F1BD1A')

    ax1.bar(bar_l,
            # using the post_score data
            gb['Switch_train'],
            # set the width
            width=bar_width,
            # with pre_score and mid_score on the bottom
            bottom=[i+j+k for i,j,k in zip(gb['Same_test'],gb['Same_training'],gb['Switch_test'])],    
            # with the label post score
            label='New (Pre-labeled)',
            # with alpha 0.5
            alpha=0.5,
            # with color
            color='#33FFF0')
    gb.to_csv('/home/msweeten/Dropbox/gb.csv')
    plt.xticks(tick_pos, gb['Genre'])
    ax1.set_ylabel('Total in Subgenre')
    ax1.set_xlabel('Subgenre')
    plt.legend(loc='upper center')
    plt.xlim([min(tick_pos)-bar_width, max(tick_pos)+bar_width])
    fig.autofmt_xdate()
    plt.savefig('/home/msweeten/insight/app/static/images/bargraph.pdf')
if __name__ == '__main__':
    con, engine = load_data_sql()
    maj_rule, plur_rule = community_databases(con)
    plot_maj_rule(maj_rule)
    
