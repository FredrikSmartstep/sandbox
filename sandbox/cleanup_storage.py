
from openai import OpenAI
import json

from secret import secrets

client = OpenAI(api_key=secrets.open_ai_key, timeout=60.0)

def delete_file(file):
    response = client.files.delete(file['id'])
    print('Deleted file ' + file['filename'] + ': ' + str(response))

def delete_vs(vs):
    print(vs.status)
    response = client.vector_stores.delete(vector_store_id=vs.id)
    print('Deleted vector store: ' + vs.name + ': ' + str(response))

def delete_assistant(assistant):
    response = client.beta.assistants.delete(assistant.id)
    print('Deleted assistant' + str(response))

if 1:
    k=0
    files = client.files.list()
    for f in files:
        k=k+1
        print('deleting file nr ' + str(k))
        f_json = f.to_json()
        print(f_json)
        delete_file(json.loads(f_json))

    k=0
    nr_of_files = 100
    while nr_of_files>0:
        nr_of_files = len(client.vector_stores.list(limit="100").data)
        for vs in client.vector_stores.list(limit="100"):
            k=k+1
            if (k%100)==0: # for some reason it breaks down and says that the last does not wexist after self providing thew id?!
                continue
            print('deleting vs nr ' + str(k))
            try:
                delete_vs(vs)
            except Exception as e:
                print(e) 

k=0
nr_of_ass = 100
while nr_of_ass>0:
    nr_of_ass = len(client.beta.assistants.list(limit="100").data)
    for c in client.beta.assistants.list(order="desc", limit="100"):
        k=k+1
        if (k%100)==0: # for some reason it breaks down and says that the last does not wexist after self providing thew id?!
            continue
        print('deleting assistant nr ' + str(k))
        delete_assistant(c)