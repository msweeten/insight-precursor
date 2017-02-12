from igraph import *

network1 = Graph()

network1.add_vertex('Fur Elise')
network1.vs["label"] = network1.vs["name"]

network2 = Graph()
network2.add_vertex('Fur Elise')
network2.add_vertex('Clair de Lune')
network2.add_edge("Fur Elise", "Clair de Lune")
network2.vs["label"] = network2.vs["name"]

edge_weights2 = [1]
network2.es["weights"] = edge_weights2
network2.es["label"] = network2.es["weights"]

trio = Graph()
trio.add_vertex('Fur Elise')
trio.add_vertex('Clair de Lune')
trio.add_vertex('Piano Sonata No.32')
trio.add_edge("Fur Elise", "Piano Sonata No.32")
trio.add_edge("Fur Elise", "Clair de Lune")#50best piano classical music pieces piano favorites
edge_weights3 = [1, 2]
trio.es["weights"]= edge_weights3
trio.es["label"] = trio.es["weights"]
trio.vs["label"] = trio.vs["name"]

layout = network1.layout('kk')
layout = network2.layout('fr')
layout = trio.layout('fr')
visual_style = {}

visual_style['vertex_size'] = 25
visual_style['vertex_color'] = 'cyan'
visual_style["edge_width"] = (1)
visual_style["edge_length"] = 5
visual_style['layout'] = layout
visual_style["margin"] = 50
visual_style["bbox"] = (400, 400)

plot(network1, '/home/msweeten/insight/app/static/images/Singleton.png', **visual_style)
plot(network2, '/home/msweeten/insight/app/static/images/Duo.png', **visual_style)
plot(trio, '/home/msweeten/insight/app/static/images/Trio.png', **visual_style)

