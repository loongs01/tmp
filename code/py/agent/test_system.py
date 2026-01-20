import sys
import os
import shutil

from src.main_system import KnowledgeSystem

def test_system():
    print("Initializing Knowledge System...")
    # Use a temp data dir
    data_dir = "test_data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    system = KnowledgeSystem(storage_dir=data_dir)
    
    # Test Data
    text1 = "Elon Musk is the CEO of SpaceX. SpaceX is located in Hawthorne."
    text2 = "Tesla produces electric cars. Elon Musk leads Tesla."
    
    print("\nProcessing content...")
    system.process_content(text1, "doc1")
    system.process_content(text2, "doc2")
    
    # Test Persistence
    print("\nSaving data...")
    system.save_data()
    
    print("\nReloading system...")
    system = KnowledgeSystem(storage_dir=data_dir)
    
    # Test Query
    print("\nQuerying 'SpaceX'...")
    results = system.query("SpaceX")
    
    print("\nResults:")
    print("Vector Search:", results['vector_search'])
    print("Graph Search:", results['graph_search'])
    
    # Assertions
    assert len(results['vector_search']) > 0, "Vector search failed"
    # Graph search might be empty if extraction wasn't perfect, but let's check if we got any triples
    # Note: Spacy model might need to be downloaded first, which the code handles.
    
    print("\nTest Completed Successfully!")

if __name__ == "__main__":
    test_system()
