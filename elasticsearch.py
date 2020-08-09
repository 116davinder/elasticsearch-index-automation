import os
import json
from elasticsearch import Elasticsearch

class ES:

    def __init__(self):
        self.es = Elasticsearch(['http://elastic01.example.com:9200'])
    
        try:
            self.es.ping()
        except Exception as e:
            print(f"Fail to Connect with Elasticsearch: {e}")

        self.es_config_path = os.environ.get('esConfigFile')
        if not self.es_config_path:
            print(f'Failed to read esConfigFile from environment')
            # exit(1)

        self.service = os.environ.get('serviceName')
        if not self.service:
            print(f'Failed to read serviceName from environment')
            # exit(1)

    def create_index(self):
        try:
            with open("elasticsearch_index.json", "r") as index_json:
                data = json.load(index_json)
                for index in data["es_indexes"]:
                    if "index_name" in index:
                        index_name = self.service + "." + index["index_name"]
                        shards = index["shards"] if "shards" in index else 10
                        replicas = index["replicas"] if "replicas" in index else 2


                        if shards < 5:
                            shards = 10
                        if replicas != 2:
                            replicas = 2
                        
                        body = { "settings":
                                    {
                                        "number_of_shards": shards,
                                        "number_of_replicas": replicas
                                    }
                                }
                        try:
                            es_index_result = self.es.indices.create(index=index_name, body=body, ignore=400)
                            print(es_index_result)
                        except Exception as e:
                            print(f"Index Creating Failed: {e}")
                    else:
                        print(f'No index_name provided, skipping')
        except FileNotFoundError:
            print(f'Failed to find elasticsearch_index.json')
        except KeyError:
            print(f'Failed to load elasticsearch_index.json, there is Key error')

if __name__ == "__main__":
    e = ES()
    e.create_index()
