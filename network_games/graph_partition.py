# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 16:48:19 2016

@author: d29905p
"""


old_graph = {
    0: [(0, 1), (0, 2), (0, 3)],
    1: [],
    2: [(2, 1)],
    3: [(3, 4), (3, 5)],
    4: [(4, 3), (4, 5)],
    5: [(5, 3), (5, 4)],
    6: [(6, 8)],
    7: [(7,8)],
    8: [(8, 9),(8,7)],
    9: []}



def connected_components(neighbors):
    seen = set()
    def component(node):
        nodes = set([node])
        while nodes:
            node = nodes.pop()
            seen.add(node)
            nodes |= neighbors[node] - seen
            yield node
    for node in neighbors:
        if node not in seen:
            yield component(node) 
            
            
new_graph = {node: set(each for edge in edges for each in edge)
             for node, edges in old_graph.items()}
components = []
for component in connected_components(new_graph):
    c = set(component)
    components.append([edge for edges in old_graph.values()
                            for edge in edges
                            if c.intersection(edge)])
print(components)




def connected_components(neighbors):
    components = []
    seen = set()
    def component(node):
        subgraph = []
        nodes = set([node])
        while nodes:
            node = nodes.pop()
            seen.add(node)
            nodes |= neighbors[node] - seen
            subgraph.append(node)
        return list(set(subgraph))
    for node in neighbors:
        if node not in seen:
            components.append(component(node)) 
    return components