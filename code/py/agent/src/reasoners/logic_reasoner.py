import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogicReasoner:
    def __init__(self, graph_builder):
        """
        Initialize the Logic Reasoner with a GraphBuilder instance.
        """
        self.graph_builder = graph_builder

    def infer_transitive(self, start_node, relation, steps=2):
        """
        Infer transitive relationships.
        e.g., if A -> B and B -> C, then A -> C (if relation is transitive).
        This is a simplified BFS search.
        """
        # For simplicity, we assume the relation is transitive and search for reachable nodes
        # strictly following edges with this relation.
        
        visited = set()
        queue = [(start_node, 0)]
        inferred = []

        while queue:
            current, depth = queue.pop(0)
            if depth >= steps:
                continue
            
            if current not in visited:
                visited.add(current)
                neighbors = self.graph_builder.query_graph(current, relation)
                
                for neighbor, rel in neighbors:
                    if neighbor not in visited:
                        inferred.append(neighbor)
                        queue.append((neighbor, depth + 1))
        
        return list(set(inferred))

    def infer_symmetric(self, node, relation):
        """
        Find nodes that have a symmetric relationship with the given node.
        e.g., if A is sibling of B, check if B is sibling of A.
        This function just returns the connected nodes, assuming the relation implies symmetry.
        """
        return [n for n, r in self.graph_builder.query_graph(node, relation)]

    def answer_question(self, question):
        """
        Placeholder for complex question answering logic.
        """
        pass

if __name__ == "__main__":
    # Test
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    if project_root not in sys.path:
        sys.path.append(project_root)
        
    from src.builders.graph_builder import GraphBuilder
    gb = GraphBuilder()
    gb.add_triples([("A", "related_to", "B"), ("B", "related_to", "C")])
    reasoner = LogicReasoner(gb)
    print("Transitive inference from A:", reasoner.infer_transitive("A", "related_to"))
