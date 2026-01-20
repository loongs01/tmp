import os
import sys

# Ensure src is in path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.main_system import KnowledgeSystem

def main():
    # Define paths
    doc_dir = r"d:\note\code\py\document"
    storage_dir = r"d:\note\code\py\agent\data"
    
    print(f"Initializing Knowledge System with storage in: {storage_dir}")
    system = KnowledgeSystem(storage_dir=storage_dir)
    
    print(f"Loading documents from: {doc_dir}")
    system.load_documents(doc_dir)
    
    print("\nSaving system state...")
    system.save_data()
    
    print("\n--- Data Sources Summary ---")
    # Access internal vector store documents to list sources
    # In our implementation, metadata is stored in system.vector_store.documents
    sources = system.vector_store.documents
    
    # Deduplicate sources just in case
    unique_sources = sorted(list(set(sources)))
    
    output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports', 'data_sources.md')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Data Sources Summary\n\n")
        f.write(f"Total Documents Processed: {len(unique_sources)}\n\n")
        for i, source in enumerate(unique_sources, 1):
            line = f"{i}. {source}"
            print(line)
            f.write(f"{line}\n")
            
    print(f"\nData sources list saved to: {output_file}")

if __name__ == "__main__":
    main()
