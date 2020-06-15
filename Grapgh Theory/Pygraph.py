#coding:utf-8
#Data:2020-06-15 22:50:00
#Python: 2.7

import copy

#--------------------Exceptions---------------------------------------------------------------
"""Exceptions used by the library."""

class PygraphError(Exception):
    """Root exception class for all library exceptions. Only used for subclassing."""
    pass


class NonexistentNodeError(PygraphError):
    """Thrown when a node does not exist within a graph."""
    def __init__(self, node_id):
        self.node_id = node_id

    def __str__(self):
        return 'Node "{}" does not exist.'.format(self.node_id)


class NonexistentEdgeError(PygraphError):
    """Thrown when an edge does not exist within a graph."""
    def __init__(self, edge_id):
        self.edge_id = edge_id

    def __str__(self):
        return 'Edge "{}" does not exist.'.format(self.edge_id)


class DisconnectedGraphError(PygraphError):
    """Thrown when a graph is disconnected (and such is unexpected by an algorithm)."""
    pass

#------------------Implements the functionality of a directed graph.----------------------------------

class DirectedGraph(object):
    nodes = None
    edges = None
    next_node_id = 1
    next_edge_id = 1

    def __init__(self):
        self.nodes = {}
        self.edges = {}
        self._num_nodes = 0#图中节点总个数
        self._num_edges = 0#图中边的总个数

    def __deepcopy__(self, memo=None):
        graph = DirectedGraph()
        graph.nodes = copy.deepcopy(self.nodes)
        graph.edges = copy.deepcopy(self.edges)
        graph.next_node_id = self.next_node_id
        graph.next_edge_id = self.next_edge_id
        graph._num_nodes = self._num_nodes
        graph._num_edges = self._num_edges
        return graph

    def num_nodes(self):
        """Returns the current number of nodes in the graph."""
        return self._num_nodes

    def num_edges(self):
        """Returns the current number of edges in the graph."""
        return self._num_edges

    def generate_node_id(self):
        node_id = self.next_node_id
        self.next_node_id += 1
        return node_id

    def generate_edge_id(self):
        edge_id = self.next_edge_id
        self.next_edge_id += 1
        return edge_id

    def new_node(self):
        """Adds a new, blank node to the graph.
        Returns the node id of the new node."""
        node_id = self.generate_node_id()

        node = {'id': node_id,
                'edges': [],
                'data': {}
        }

        self.nodes[node_id] = node

        self._num_nodes += 1

        return node_id

    def new_edge(self, node_a, node_b, cost=1):
        """Adds a new edge from node_a to node_b that has a cost.
        Returns the edge id of the new edge."""

        # Verify that both nodes exist in the graph
        try:
            self.nodes[node_a]
        except KeyError:
            raise NonexistentNodeError(node_a)
        try:
            self.nodes[node_b]
        except KeyError:
            raise NonexistentNodeError(node_b)

        # Create the new edge
        edge_id = self.generate_edge_id()

        edge = {'id': edge_id,
                'vertices': (node_a, node_b),
                'cost': cost,
                'data': {}
        }

        self.edges[edge_id] = edge
        self.nodes[node_a]['edges'].append(edge_id)

        self._num_edges += 1

        return edge_id

    def neighbors(self, node_id):
        """Find all the nodes where there is an edge from the specified node to that node.
        Returns a list of node ids."""
        node = self.get_node(node_id)
        return [self.get_edge(edge_id)['vertices'][1] for edge_id in node['edges']]

    def adjacent(self, node_a, node_b):
        """Determines whether there is an edge from node_a to node_b.
        Returns True if such an edge exists, otherwise returns False."""
        neighbors = self.neighbors(node_a)
        return node_b in neighbors

    def edge_cost(self, node_a, node_b):
        """Returns the cost of moving between the edge that connects node_a to node_b.
        Returns +inf if no such edge exists."""
        cost = float('inf')
        node_object_a = self.get_node(node_a)
        for edge_id in node_object_a['edges']:
            edge = self.get_edge(edge_id)
            tpl = (node_a, node_b)
            if edge['vertices'] == tpl:
                cost = edge['cost']
                break
        return cost

    def get_node(self, node_id):
        """Returns the node object identified by "node_id"."""
        try:
            node_object = self.nodes[node_id]
        except KeyError:
            raise NonexistentNodeError(node_id)
        return node_object

    def get_all_node_ids(self):
        """Returns a list of all the node ids in the graph."""
        return self.nodes.keys()

    def get_all_node_objects(self):
        """Returns a list of all the node objects in the graph."""
        return self.nodes.values()

    def get_edge(self, edge_id):
        """Returns the edge object identified by "edge_id"."""
        try:
            edge_object = self.edges[edge_id]
        except KeyError:
            raise NonexistentEdgeError(edge_id)
        return edge_object

    def get_all_edge_ids(self):
        """Returns a list of all the edge ids in the graph"""
        return self.edges.keys()

    def get_all_edge_objects(self):
        """Returns a list of all the edge objects in the graph."""
        return self.edges.values()

    def delete_edge_by_id(self, edge_id):
        """Removes the edge identified by "edge_id" from the graph."""
        edge = self.get_edge(edge_id)

        # Remove the edge from the "from node"
        # --Determine the from node
        from_node_id = edge['vertices'][0]
        from_node = self.get_node(from_node_id)

        # --Remove the edge from it
        from_node['edges'].remove(edge_id)

        # Remove the edge from the edge list
        del self.edges[edge_id]

        self._num_edges -= 1

    def delete_edge_by_nodes(self, node_a, node_b):
        """Removes all the edges from node_a to node_b from the graph."""
        node = self.get_node(node_a)

        # Determine the edge ids
        edge_ids = []
        for e_id in node['edges']:
            edge = self.get_edge(e_id)
            if edge['vertices'][1] == node_b:
                edge_ids.append(e_id)

        # Delete the edges
        for e in edge_ids:
            self.delete_edge_by_id(e)

    def delete_node(self, node_id):
        """Removes the node identified by node_id from the graph."""
        node = self.get_node(node_id)

        # Remove all edges from the node
        for e in node['edges']:
            self.delete_edge_by_id(e)

        # Remove all edges to the node
        edges = [edge_id for edge_id, edge in self.edges.items() if edge['vertices'][1] == node_id]
        for e in edges:
            self.delete_edge_by_id(e)

        # Remove the node from the node list
        del self.nodes[node_id]

        self._num_nodes -= 1

    def move_edge_source(self, edge_id, node_a, node_b):
        """Moves an edge originating from node_a so that it originates from node_b."""
        # Grab the edge
        edge = self.get_edge(edge_id)

        # Alter the vertices
        edge['vertices'] = (node_b, edge['vertices'][1])

        # Remove the edge from node_a
        node = self.get_node(node_a)
        node['edges'].remove(edge_id)

        # Add the edge to node_b
        node = self.get_node(node_b)
        node['edges'].append(edge_id)

    def move_edge_target(self, edge_id, node_a):
        """Moves an edge so that it targets node_a."""
        # Grab the edge
        edge = self.get_edge(edge_id)

        # Alter the vertices
        edge['vertices'] = (edge['vertices'][0], node_a)

    def get_edge_ids_by_node_ids(self, node_a, node_b):
        """Returns a list of edge ids connecting node_a to node_b."""
        # Check if the nodes are adjacent
        if not self.adjacent(node_a, node_b):
            return []

        # They're adjacent, so pull the list of edges from node_a and determine which ones point to node_b
        node = self.get_node(node_a)
        return [edge_id for edge_id in node['edges'] if self.get_edge(edge_id)['vertices'][1] == node_b]

    def get_first_edge_id_by_node_ids(self, node_a, node_b):
        """Returns the first (and possibly only) edge connecting node_a and node_b."""
        ret = self.get_edge_ids_by_node_ids(node_a, node_b)
        if not ret:
            return None
        else:
            return ret[0]
#------------------Implements the functionality of an undirected graph----------------------------------

class UndirectedGraph(DirectedGraph):
    def __deepcopy__(self, memo=None):
        graph = UndirectedGraph()
        graph.nodes = copy.deepcopy(self.nodes)
        graph.edges = copy.deepcopy(self.edges)
        graph.next_node_id = self.next_node_id
        graph.next_edge_id = self.next_edge_id
        graph._num_nodes = self._num_nodes
        graph._num_edges = self._num_edges
        return graph

    def new_edge(self, node_a, node_b, cost=1):
        """Adds a new, undirected edge between node_a and node_b with a cost.
        Returns the edge id of the new edge."""
        edge_id = super(UndirectedGraph, self).new_edge(node_a, node_b, cost)
        self.nodes[node_b]['edges'].append(edge_id)
        return edge_id

    def neighbors(self, node_id):
        """Find all the nodes where there is an edge from the specified node to that node.
        Returns a list of node ids."""
        node = self.get_node(node_id)
        flattened_nodes_list = []
        for a, b in [self.get_edge(edge_id)['vertices'] for edge_id in node['edges']]:
            flattened_nodes_list.append(a)
            flattened_nodes_list.append(b)
        node_set = set(flattened_nodes_list)
        if node_id in node_set:
            node_set.remove(node_id)
        return [nid for nid in node_set]

    def delete_edge_by_id(self, edge_id):
        """Removes the edge identified by "edge_id" from the graph."""
        edge = self.get_edge(edge_id)

        # Remove the edge from the "from node"
        # --Determine the from node
        from_node_id = edge['vertices'][0]
        from_node = self.get_node(from_node_id)

        # --Remove the edge from it
        from_node['edges'].remove(edge_id)

        # Remove the edge from the "to node"
        to_node_id = edge['vertices'][1]
        to_node = self.get_node(to_node_id)

        # --Remove the edge from it
        to_node['edges'].remove(edge_id)

        # Remove the edge from the edge list
        del self.edges[edge_id]

        self._num_edges -= 1

    def move_edge_target(self, edge_id, node_a):
        """Moves an edge so that it targets node_a."""
        # Grab the edge
        edge = self.get_edge(edge_id)

        # Remove the edge from the original "target node"
        original_target_node_id = edge['vertices'][1]
        original_target_node = self.get_node(original_target_node_id)
        original_target_node['edges'].remove(edge_id)

        # Add the edge to the new target node
        new_target_node_id = node_a
        new_target_node = self.get_node(new_target_node_id)
        new_target_node['edges'].append(edge_id)

        # Alter the vertices on the edge
        edge['vertices'] = (edge['vertices'][0], node_a)

    def get_edge_ids_by_node_ids(self, node_a, node_b):
        """Returns a list of edge ids connecting node_a to node_b."""
        # Check if the nodes are adjacent
        if not self.adjacent(node_a, node_b):
            return []

        # They're adjacent, so pull the list of edges from node_a and determine which ones point to node_b
        node = self.get_node(node_a)
        return [edge_id for edge_id in node['edges'] \
                if (self.get_edge(edge_id)['vertices'][1] == node_b or self.get_edge(edge_id)['vertices'][0] == node_b)]

#------------------Helper Function for graph------------------------------------------------------------------------------------------------

# Graph Conversions

def make_subgraph(graph, vertices, edges):
    """Converts a subgraph given by a list of vertices and edges into a graph object."""
    # Copy the entire graph
    local_graph = copy.deepcopy(graph)

    # Remove all the edges that aren't in the list
    edges_to_delete = filter(lambda x: x not in edges, local_graph.get_all_edge_ids())
    for e in edges_to_delete:
        local_graph.delete_edge_by_id(e)

    # Remove all the vertices that aren't in the list
    nodes_to_delete = filter(lambda x: x not in vertices, local_graph.get_all_node_ids())
    for n in nodes_to_delete:
        local_graph.delete_node(n)

    return local_graph


def convert_graph_directed_to_undirected(dg):
    """Converts a directed graph into an undirected graph. Directed edges are made undirected."""

    udg = UndirectedGraph()

    # Copy the graph
    # --Copy nodes
    # --Copy edges
    udg.nodes = copy.deepcopy(dg.nodes)
    udg.edges = copy.deepcopy(dg.edges)
    udg.next_node_id = dg.next_node_id
    udg.next_edge_id = dg.next_edge_id

    # Convert the directed edges into undirected edges
    for edge_id in udg.get_all_edge_ids():
        edge = udg.get_edge(edge_id)
        target_node_id = edge['vertices'][1]
        target_node = udg.get_node(target_node_id)
        target_node['edges'].append(edge_id)

    return udg


def remove_duplicate_edges_directed(dg):
    """Removes duplicate edges from a directed graph."""
    # With directed edges, we can just hash the to and from node id tuples and if
    # a node happens to conflict with one that already exists, we delete it

    # --For aesthetic, we sort the edge ids so that lower edge ids are kept
    lookup = {}
    edges = sorted(dg.get_all_edge_ids())
    for edge_id in edges:
        e = dg.get_edge(edge_id)
        tpl = e['vertices']
        if tpl in lookup:
            dg.delete_edge_by_id(edge_id)
        else:
            lookup[tpl] = edge_id


def remove_duplicate_edges_undirected(udg):
    """Removes duplicate edges from an undirected graph."""
    # With undirected edges, we need to hash both combinations of the to-from node ids, since a-b and b-a are equivalent
    # --For aesthetic, we sort the edge ids so that lower edges ids are kept
    lookup = {}
    edges = sorted(udg.get_all_edge_ids())
    for edge_id in edges:
        e = udg.get_edge(edge_id)
        tpl_a = e['vertices']
        tpl_b = (tpl_a[1], tpl_a[0])
        if tpl_a in lookup or tpl_b in lookup:
            udg.delete_edge_by_id(edge_id)
        else:
            lookup[tpl_a] = edge_id
            lookup[tpl_b] = edge_id


def get_vertices_from_edge_list(graph, edge_list):
    """Transforms a list of edges into a list of the nodes those edges connect.
    Returns a list of nodes, or an empty list if given an empty list.
    """
    node_set = set()
    for edge_id in edge_list:
        edge = graph.get_edge(edge_id)
        a, b = edge['vertices']
        node_set.add(a)
        node_set.add(b)

    return list(node_set)


def get_subgraph_from_edge_list(graph, edge_list):
    """Transforms a list of edges into a subgraph."""
    node_list = get_vertices_from_edge_list(graph, edge_list)
    subgraph = make_subgraph(graph, node_list, edge_list)

    return subgraph


def merge_graphs(main_graph, addition_graph):
    """Merges an ''addition_graph'' into the ''main_graph''.
    Returns a tuple of dictionaries, mapping old node ids and edge ids to new ids.
    """

    node_mapping = {}
    edge_mapping = {}

    for node in addition_graph.get_all_node_objects():
        node_id = node['id']
        new_id = main_graph.new_node()
        node_mapping[node_id] = new_id

    for edge in addition_graph.get_all_edge_objects():
        edge_id = edge['id']
        old_vertex_a_id, old_vertex_b_id = edge['vertices']
        new_vertex_a_id = node_mapping[old_vertex_a_id]
        new_vertex_b_id = node_mapping[old_vertex_b_id]
        new_edge_id = main_graph.new_edge(new_vertex_a_id, new_vertex_b_id)
        edge_mapping[edge_id] = new_edge_id

    return node_mapping, edge_mapping


def create_graph_from_adjacency_matrix(adjacency_matrix):
    """Generates a graph from an adjacency matrix specification.
       Returns a tuple containing the graph and a list-mapping of node ids to matrix column indices.

       The graph will be an UndirectedGraph if the provided adjacency matrix is symmetric.
       The graph will be a DirectedGraph if the provided adjacency matrix is not symmetric.
       Ref: http://mathworld.wolfram.com/AdjacencyMatrix.html"""
    if is_adjacency_matrix_symmetric(adjacency_matrix):
        graph = UndirectedGraph()
    else:
        graph = DirectedGraph()

    node_column_mapping = []

    num_columns = len(adjacency_matrix)
    for _ in xrange(num_columns):
        node_id = graph.new_node()
        node_column_mapping.append(node_id)

    for j in xrange(num_columns):
        for i in xrange(num_columns):
            if adjacency_matrix[j][i]:
                jnode_id = node_column_mapping[j]
                inode_id = node_column_mapping[i]
                # Because of our adjacency matrix encoding, [j][i] in our code corresponds to [i][j] in a traditional matrix interpretation
                # Thus, we need to put an edge from node i to node j if [j][i] in our code is non-zero
                graph.new_edge(inode_id, jnode_id)

    return (graph, node_column_mapping)


def is_adjacency_matrix_symmetric(adjacency_matrix):
    """Determines if an adjacency matrix is symmetric.
       Ref: http://mathworld.wolfram.com/SymmetricMatrix.html"""
    # Verify that the matrix is square
    num_columns = len(adjacency_matrix)
    for column in adjacency_matrix:
        # In a square matrix, every row should be the same length as the number of columns
        if len(column) != num_columns:
            return False

    # Loop through the bottom half of the matrix and compare it to the top half
    # --We do the bottom half because of how we construct adjacency matrices
    max_i = 0
    for j in xrange(num_columns):
        for i in xrange(max_i):
            # If i == j, we can skip ahead so we don't compare with ourself
            if i == j:
                continue
            # Compare the value in the bottom half with the mirrored value in the top half
            # If they aren't the same, the matrix isn't symmetric
            if adjacency_matrix[j][i] != adjacency_matrix[i][j]:
                return False
        max_i += 1

    # If we reach this far without returning false, then we know that everything matched,
    # which makes this a symmetric matrix
    return True

def graph_to_dot(graph, node_renderer=None, edge_renderer=None):
    """Produces a DOT specification string from the provided graph."""
    node_pairs = graph.nodes.items()
    edge_pairs = graph.edges.items()

    if node_renderer is None:
        node_renderer_wrapper = lambda nid: ''
    else:
        node_renderer_wrapper = lambda nid: ' [%s]' % ','.join(
            map(lambda tpl: '%s=%s' % tpl, node_renderer(graph, nid).items()))

    # Start the graph
    graph_string = 'digraph G {\n'
    graph_string += 'overlap=scale;\n'

    # Print the nodes (placeholder)
    for node_id, node in node_pairs:
        graph_string += '%i%s;\n' % (node_id, node_renderer_wrapper(node_id))

    # Print the edges
    for edge_id, edge in edge_pairs:
        node_a = edge['vertices'][0]
        node_b = edge['vertices'][1]
        graph_string += '%i -> %i;\n' % (node_a, node_b)

    # Finish the graph
    graph_string += '}'

    return graph_string
if __name__ == '__main__':
    adjacency_matrix = [[1,0,1,0,0,0,0],
                  [0,1,0,0,0,0,0],
                  [0,0,1,1,0,0,0],
                  [0,0,0,0,1,0,0],
                  [1,0,0,0,0,0,1],
                  [0,0,0,0,0,1,0],
                  [0,0,0,1,0,0,1]]
    graph, c = create_graph_from_adjacency_matrix(adjacency_matrix)
    print(graph_to_dot(graph))
