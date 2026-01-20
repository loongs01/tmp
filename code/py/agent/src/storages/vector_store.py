import faiss
import numpy as np
import pickle
import os
import logging
from collections import Counter
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorStore:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        """
        Initialize the Vector Store with a Sentence Transformer model.
        Falls back to TF-IDF (scikit-learn) or simple word overlap if Sentence Transformers fails.
        """
        self.use_transformer = False
        self.use_tfidf = False
        self.documents = [] # Store metadata
        self.texts = [] # Store raw text content (needed for TF-IDF fallback)
        self.index = None
        self.dimension = 0
        
        # Try loading Sentence Transformer
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(model_name)
            self.dimension = self.model.get_sentence_embedding_dimension()
            self.index = faiss.IndexFlatL2(self.dimension)
            self.use_transformer = True
            logger.info(f"Initialized VectorStore with model: {model_name}, dimension: {self.dimension}")
        except Exception as e:
            logger.error(f"Failed to load SentenceTransformer: {e}. Attempting fallback...")
            self.use_transformer = False

        # Fallback: TF-IDF
        if not self.use_transformer:
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.metrics.pairwise import cosine_similarity
                self.vectorizer = TfidfVectorizer(stop_words='english')
                self.tfidf_matrix = None
                self.use_tfidf = True
                logger.info("Initialized VectorStore with TF-IDF fallback.")
            except ImportError:
                logger.warning("scikit-learn not found. Switching to simple word overlap search.")
                self.use_tfidf = False
                self.doc_vectors = []

    def add_documents(self, texts, metadata=None):
        """
        Embed texts and add them to the index.
        """
        if not texts:
            return
        
        # Always store texts if we might need them for TF-IDF
        if not self.use_transformer:
            self.texts.extend(texts)

        if self.use_transformer:
            embeddings = self.model.encode(texts)
            self.index.add(np.array(embeddings).astype('float32'))
        elif self.use_tfidf:
            # Lazy update
            pass 
        else:
            # Simple Bag of Words / TF-IDF ish
            for text in texts:
                vec = self._text_to_vector(text)
                self.doc_vectors.append(vec)
        
        if metadata:
            self.documents.extend(metadata)
        else:
            self.documents.extend(texts)
            
        logger.info(f"Added {len(texts)} documents to the index.")

    def search(self, query, k=5):
        """
        Search for the top-k most similar documents to the query.
        Returns a list of (document, distance).
        """
        results = []
        
        if self.use_transformer:
            query_vector = self.model.encode([query])
            distances, indices = self.index.search(np.array(query_vector).astype('float32'), k)
            
            for i, idx in enumerate(indices[0]):
                if idx != -1 and idx < len(self.documents):
                    results.append((self.documents[idx], float(distances[0][i])))
        elif self.use_tfidf:
            from sklearn.metrics.pairwise import cosine_similarity
            # Re-vectorize all documents + query (inefficient but robust fallback)
            # Use self.texts which contains the actual content
            if not self.texts:
                return []
                
            all_texts = self.texts + [query]
            try:
                tfidf_matrix = self.vectorizer.fit_transform(all_texts)
                query_vec = tfidf_matrix[-1]
                doc_vecs = tfidf_matrix[:-1]
                
                cosine_sims = cosine_similarity(query_vec, doc_vecs).flatten()
                # Get top k indices
                top_k_indices = cosine_sims.argsort()[-k:][::-1]
                
                for idx in top_k_indices:
                    if cosine_sims[idx] > 0:
                        # Ensure idx is within bounds of documents
                        if idx < len(self.documents):
                            results.append((self.documents[idx], float(cosine_sims[idx])))
            except ValueError:
                # Handle empty vocab or other issues
                pass
        else:
            # Simple Cosine Similarity
            query_vec = self._text_to_vector(query)
            scores = []
            for i, doc_vec in enumerate(self.doc_vectors):
                score = self._cosine_similarity(query_vec, doc_vec)
                scores.append((i, score))
            
            # Sort by score descending
            scores.sort(key=lambda x: x[1], reverse=True)
            
            for i, score in scores[:k]:
                results.append((self.documents[i], score))
        
        return results

    def _text_to_vector(self, text):
        words = text.lower().split()
        return Counter(words)

    def _cosine_similarity(self, vec1, vec2):
        intersection = set(vec1.keys()) & set(vec2.keys())
        numerator = sum([vec1[x] * vec2[x] for x in intersection])
        
        sum1 = sum([vec1[x]**2 for x in vec1.keys()])
        sum2 = sum([vec2[x]**2 for x in vec2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)
        
        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

    def save_index(self, path_prefix):
        """
        Save the index and documents metadata.
        """
        try:
            os.makedirs(os.path.dirname(path_prefix), exist_ok=True)
            with open(f"{path_prefix}.meta", 'wb') as f:
                pickle.dump(self.documents, f)
            
            # Save texts if using fallback
            if not self.use_transformer:
                with open(f"{path_prefix}.texts", 'wb') as f:
                    pickle.dump(self.texts, f)

            if self.use_transformer:
                faiss.write_index(self.index, f"{path_prefix}.index")
            elif self.use_tfidf:
                pass
            else:
                with open(f"{path_prefix}.simple_index", 'wb') as f:
                    pickle.dump(self.doc_vectors, f)
                    
            logger.info(f"Index saved to {path_prefix}")
        except Exception as e:
            logger.error(f"Failed to save index: {e}")

    def load_index(self, path_prefix):
        """
        Load the index and documents metadata.
        """
        try:
            if os.path.exists(f"{path_prefix}.meta"):
                with open(f"{path_prefix}.meta", 'rb') as f:
                    self.documents = pickle.load(f)
                
                # Load texts if available
                if os.path.exists(f"{path_prefix}.texts"):
                    with open(f"{path_prefix}.texts", 'rb') as f:
                        self.texts = pickle.load(f)

                if os.path.exists(f"{path_prefix}.index"):
                    # Try loading FAISS
                    try:
                        self.index = faiss.read_index(f"{path_prefix}.index")
                        # Check if transformer is actually usable
                        if not self.use_transformer:
                            logger.warning("Loaded FAISS index but Transformer model failed. Cannot use index.")
                    except Exception as e:
                        logger.error(f"Failed to load FAISS index: {e}")
                
                elif os.path.exists(f"{path_prefix}.simple_index"):
                     with open(f"{path_prefix}.simple_index", 'rb') as f:
                        self.doc_vectors = pickle.load(f)
                
                logger.info(f"Index loaded from {path_prefix}")
            else:
                logger.warning(f"Index files not found at {path_prefix}")
        except Exception as e:
            logger.error(f"Failed to load index: {e}")

if __name__ == "__main__":
    # Test
    store = VectorStore()
    texts = ["Apple is a technology company.", "Bananas are yellow fruit."]
    store.add_documents(texts)
    print("Search 'fruit':", store.search("fruit", k=1))
    store.save_index("test_index")
