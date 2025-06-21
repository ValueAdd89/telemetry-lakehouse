
import os
import json
import numpy as np
import faiss
from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List

class TelemetryRAGPipeline:
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.texts = []
        self.vectorizer = TfidfVectorizer()
        self.index = None

    def load_and_prepare_data(self):
        with open(self.json_path, 'r') as f:
            events = json.load(f)
        self.texts = [f"{e['user_id']} used {e['feature']} at {e['timestamp']}" for e in events]

    def embed_and_index(self):
        tfidf_matrix = self.vectorizer.fit_transform(self.texts)
        self.index = faiss.IndexFlatL2(tfidf_matrix.shape[1])
        self.index.add(tfidf_matrix.toarray())

    def query(self, query_text: str, k: int = 3) -> List[str]:
        query_vec = self.vectorizer.transform([query_text]).toarray().astype('float32')
        distances, indices = self.index.search(query_vec, k)
        return [self.texts[i] for i in indices[0]]

if __name__ == "__main__":
    rag = TelemetryRAGPipeline("data/sample_events.json")
    rag.load_and_prepare_data()
    rag.embed_and_index()
    print("Top results for query: 'user u1 used search'")
    results = rag.query("user u1 used search")
    for res in results:
        print(res)
