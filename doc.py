from haystack.document_stores import FAISSDocumentStore


document_store = FAISSDocumentStore.load("bms")

for e in document_store:
    print(e.content)
