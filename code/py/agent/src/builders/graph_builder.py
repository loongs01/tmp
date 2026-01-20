import networkx as nx
import pickle
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GraphBuilder:
    def __init__(self):
        """
        Initialize the Graph Builder with a directed graph.
        """
        self.graph = nx.DiGraph()

    def add_triples(self, triples):
        """
        Add a list of triples (subject, predicate, object) to the graph.
        """
        for subj, pred, obj in triples:
            self.graph.add_edge(subj, obj, relation=pred)
        logger.info(f"Added {len(triples)} triples to the graph.")

    def query_graph(self, node, relation=None, direction='both'):
        """
        Find nodes connected to the given node, optionally filtering by relation.
        Returns a list of (neighbor, relation, direction).
        """
        results = []
        if node in self.graph:
            # Outgoing
            if direction in ['out', 'both']:
                for neighbor in self.graph.neighbors(node):
                    edge_data = self.graph.get_edge_data(node, neighbor)
                    rel = edge_data.get('relation')
                    if relation is None or rel == relation:
                        results.append((neighbor, rel, 'out'))
            
            # Incoming
            if direction in ['in', 'both']:
                for predecessor in self.graph.predecessors(node):
                    edge_data = self.graph.get_edge_data(predecessor, node)
                    rel = edge_data.get('relation')
                    if relation is None or rel == relation:
                        results.append((predecessor, rel, 'in'))
        return results

    def save_graph(self, path):
        """
        Save the graph to a pickle file.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                pickle.dump(self.graph, f)
            logger.info(f"Graph saved to {path}")
        except Exception as e:
            logger.error(f"Failed to save graph: {e}")

    def load_graph(self, path):
        """
        Load the graph from a pickle file.
        """
        try:
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    self.graph = pickle.load(f)
                logger.info(f"Graph loaded from {path}")
            else:
                logger.warning(f"Graph file not found at {path}")
        except Exception as e:
            logger.error(f"Failed to load graph: {e}")

if __name__ == "__main__":
    # Test
    builder = GraphBuilder()
    triples = [("Apple", "founded_by", "Steve Jobs"), ("Apple", "located_in", "Cupertino")]
    builder.add_triples(triples)
    print("Query 'Apple':", builder.query_graph("Apple"))
    builder.save_graph("test_graph.pkl")
