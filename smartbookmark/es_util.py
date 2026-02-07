from sentence_transformers import SentenceTransformer


def build_hybrid_query(query, query_vector, doc_type=None, tags=None, top_k=22):
    filters = []
    if doc_type:
        filters.append({"term": {"doc_type": doc_type}})
    if tags:
        filters.append({"terms": {"tags": tags}})

    return {
        "knn": {
            "field": "embedding",
            "query_vector": query_vector,
            "k": top_k,
            "num_candidates": 100,
            "filter": filters
        },
        "query": {
            "bool": {
                "filter": filters,
                "should": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["title^3", "summary^2", "content"]
                        }
                    }
                ]
            }
        }
    }

_model = SentenceTransformer('all-MiniLM-L6-v2')

def embed(text: str) -> list[float]:
    embedding = _model.encode(text)
    return embedding.tolist()
	