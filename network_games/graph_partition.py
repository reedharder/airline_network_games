# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 16:48:19 2016

@author: d29905p
"""





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
print components