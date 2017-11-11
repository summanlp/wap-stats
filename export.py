
import networkx as _nx
from networkx.drawing.nx_agraph import graphviz_layout
from os import system as _shell
from pagerank_weighted import pagerank_weighted_scipy as _pagerank_weighted_scipy

NODE_COLOR = {'r': 239, 'g': 10, 'b': 10}
MAX_EDGE_WIDTH = 4
MIN_EDGE_WIDTH = 0.1


def _write_gexf(graph, scores, path="test.gexf", labels=None):
    nx_graph = _get_nx_graph(graph, scores)
    _set_layout(nx_graph, scores, labels)
    _nx.write_gexf(nx_graph, path)
    _shell("sed -i 's/<ns0/<viz/g' {0}".format(path))
    #_shell('echo \'<?xml version="1.0" encoding="UTF-8"?>\' | cat - {0} > out.tmp && mv out.tmp {0}'.format(path))
    #_shell("mv {0} views/{0}".format(path))


def transform_edge_scores(graph):
    """Transform the max score to MAX_EDGE_WIDTH, the min score to MIN_EDGE_WIDTH and all the values in between
    linearly, ignoring zeros"""
    edges = {edge: graph.edge_weight(edge) for edge in graph.edges() if graph.edge_weight(edge) != 0}

    max_score = max(edges.iteritems(), key=(lambda k: edges[k[0]]))[1]
    min_score = min(edges.iteritems(), key=(lambda k: edges[k[0]]))[1]
    old_range = max_score - min_score
    new_range = MAX_EDGE_WIDTH - MIN_EDGE_WIDTH
    map_value = lambda x: (((x - min_score) * new_range) / float(old_range)) + MIN_EDGE_WIDTH
    return {k: map_value(v) for k, v in edges.iteritems()}


def _get_nx_graph(graph, new_scores):
    nx_graph = _nx.Graph()
    nx_graph.add_nodes_from(graph.nodes())
    transformed_edges = transform_edge_scores(graph)
    for edge in transformed_edges:
        weight = transformed_edges[edge]
        if weight != 0:
            nx_graph.add_edge(edge[0], edge[1], {'weight': weight})
    return nx_graph


def _set_layout(nx_graph, scores, labels):
    positions = graphviz_layout(nx_graph, prog="neato") # prog options: neato, dot, fdp, sfdp, twopi, circo
    centered_positions = _center_positions(positions)
    for node in nx_graph.nodes():
        nx_graph.node[node]['viz'] = _get_viz_data(node, centered_positions, scores)
        label = labels[node] if labels is not None and node in labels else node
        nx_graph.node[node]['label'] = " ".join(label.split()[0:2])
        nx_graph.node[node]['id'] = label


def _center_positions(positions):
    min_x = positions[min(positions, key=lambda k:positions[k][0])][0]
    min_y = positions[min(positions, key=lambda k:positions[k][1])][1]
    max_x = positions[max(positions, key=lambda k:positions[k][0])][0]
    max_y = positions[max(positions, key=lambda k:positions[k][1])][1]
    delta_x = (min_x + max_x) / 2
    delta_y = (min_y + max_y) / 2

    centered_positions = {}
    for key, position in positions.iteritems():
        new_position = (round(position[0] - delta_x, 2), round(position[1] - delta_y, 2))
        centered_positions[key] = new_position
    return centered_positions


def _get_viz_data(node, positions, scores):
    viz_data = {}
    viz_data['position'] = {'x':positions[node][0], 'y':positions[node][1]}
    viz_data['size'] = scores[node]
    viz_data['color'] = NODE_COLOR
    return viz_data


def gexf_export_from_graph(graph, path="test.gexf", labels=None):
    scores = _pagerank_weighted_scipy(graph)
    _write_gexf(graph, scores, path, labels)
