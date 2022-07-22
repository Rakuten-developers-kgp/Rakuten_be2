from datetime import datetime, timedelta
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from fastapi import FastAPI, Request
from sqlalchemy import null
from fastapi.middleware.cors import CORSMiddleware


url = "http://localhost:9200"
user = 'elastic'
pw = 'zkEVQNOzi=gG21ZKl*dM'

def configuration():

    es = Elasticsearch(
        url,
        http_auth=(user, pw)
    )
    return es



app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


must_array =[]


# must = ['Task', 'executed']
# print(type(must[0]))
# must_query = []

# for i in must:

#     q = Q('match',  message = i )
#     must_query.insert(0,q)


# q = Q('bool', must = must_query, should = [], must_not = [], filter = [])

# s1.query = q
# s1 = s1[0:20]
# res = s1.execute().to_dict()['hits']['hits']

# print(res)

# res = s1.execute()

@app.post("/query_data")
async def query(request:Request):
    # request = {
    #     "must":['Task'],
    #     "should":['Task'],
    #     "not":['uvicorn.error']
    # }

    es = configuration()
    s1 = Search(using=es, index = "app.logs-*")


    request = await request.json()
    print(request)
    request1 = request.get('formField4')
    # print(request, 'happyyy')

    print(request1, "jaibambam")

    # print("requets dataa", request.json)

    must_params = request1.get('must', ['null'])
    must_not_params = request1.get('not', ['null'])
    should_params = request1.get('should', ['null'])
    filter = request1.get('filter', 'nothing')
    # filter = filter.get('0', 'nothing')
    re_filter = filter.get('0')
    print(type(must_params), 'jajajaja')

    limit = json.loads(request1.get('limit', '0'))
    offset = json.loads(request1.get('offset', '10'))
    print(type(offset))



    must_query =[]
    if must_params[0] != 'null':
        for i in must_params:
            q = Q('match',  message = i )
            must_query.insert(0,q)

    must_not_query=[]
    if must_not_params[0] != 'null':
        for i in must_not_params:
            q = Q('match',  message = i )
            must_not_query.insert(0,q)

    should_query = []
    if should_params[0] != 'null':
        for i in should_params:
            q = Q('match',  message = i )
            should_query.insert(0,q) 

    # if filter == 'nothing':
    # # a = int(filter.get("lte", datetime.now().timestamp())*1000)
    # # b = int(filter.get("gte", (datetime.now() - timedelta(minutes = 100)).timestamp())*1000)
    # # date_filter = { "range": { "@timestamp": { "format": "epoch_millis", "gte": int(b), "lte": int(a) } } }
    #     a = int((datetime.now().timestamp())*1000)
    #     b = int((datetime.now() - timedelta(minutes = 100000)).timestamp())*1000
    #     date_filter = { "range": { "@timestamp": { "format": "epoch_millis", "gte": int(b), "lte": int(a) } } }
    # else:
    a = re_filter.get('lte', int((datetime.now().timestamp())*1000))
    b = re_filter.get('gte', int((datetime.now() - timedelta(minutes = 100000)).timestamp())*1000)
    if a == '' and b == '':
        a = int((datetime.now().timestamp())*1000)
        b = int((datetime.now() - timedelta(minutes = 10000)).timestamp())*1000
    date_filter = { "range": { "@timestamp": { "format": "epoch_millis", "gte": int(b), "lte": int(a) } } }
    q = Q('bool', must = must_query, must_not = must_not_query , should = should_query, filter = date_filter)

    s1.query = q
    s1 = s1[limit:offset]
    res = s1.execute().to_dict()['hits']['hits']
    # print(res)

    final_res =[]
    for i in res:
        index = i.get('_index')
        id = i.get('_id')
        timestamp = i['_source'].get('@timestamp')
        message = i['_source'].get('message')
        dict = {'index':index, 'id':id, 'timestamp': timestamp, 'log': message}
        final_res.append(dict)

    return final_res