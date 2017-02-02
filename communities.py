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
