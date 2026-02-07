from elasticsearch import Elasticsearch
from constants import ES_URL, INDEX_NAME

class ESClient:
    def __init__(self):
        self.es = Elasticsearch(ES_URL)        

    def index_doc(self, doc: dict):
        return self.es.index(
            index=INDEX_NAME,
            document=doc,
            refresh="wait_for"
        )

    def search(self, body: dict, size: int=5):
        response = self.es.search(
            index=INDEX_NAME,
            body=body,
            size=size
        )
        return response["hits"]["hits"]
