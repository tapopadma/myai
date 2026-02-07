from mcp.server.fastmcp import FastMCP
from es_client import ESClient
from typing import List, Optional
import es_util


mcp = FastMCP("smartbookmark-mcp")
esclient = ESClient()

@mcp.tool()
def add_document(title: str, content: str, doc_id: Optional[str] = None, 
        doc_type: Optional[str] = None, tags: List[str] = []) -> str:
    """Add a document to the smart bookmark Elasticsearch index."""
    doc = {
        "title": title,
        "content": content,
        "doc_type": doc_type,
        "tags": tags,
        "embedding": es_util.embed(content)
    }
    new_id = esclient.index_doc(doc)
    return f"Document indexed successfully. id={new_id}"

@mcp.tool()
def search_documents(query: str, doc_type: Optional[str] = None, 
        tags: Optional[List[str]] = None, top_k: int = 5) -> str:
    """Hybrid search over stored documents."""
    query_embedding = es_util.embed(query)
    query = es_util.build_hybrid_query(
        query=query,
        query_vector=query_embedding,
        doc_type=doc_type,
        tags=tags,
        top_k=top_k
    )
    results = esclient.search(query)
    
    if not results:
        return "No documents found."

    formatted = []
    for r in results:
        source = r.get("_source", {})
        formatted.append({
            "title": source.get("title", "No Title"),
            "content": source.get("content", "No Content"),
            "score": r.get("_score")
        })
    return str(formatted)

if __name__ == '__main__':
    mcp.run()