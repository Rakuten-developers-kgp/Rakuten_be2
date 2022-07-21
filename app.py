import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q


url = "http://localhost:9200"
user = 'elastic'
pw = 'zkEVQNOzi=gG21ZKl*dM'

def configuration():

    es = Elasticsearch(
        url,
        http_auth=(user, pw)
    )
    return es


es = configuration()
s1 = Search(using=es, index = "app.logs-*")

must_array =[]



q = Q('bool', must = [Q('match',  message = 'Task')])

s1.query = q
s1 = s1[0:20]
res = s1.execute().to_dict()

print(res)

# res = s1.execute()
