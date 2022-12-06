from haystack.document_stores import FAISSDocumentStore


document_store = FAISSDocumentStore.load("234234242")

for e in document_store:
    print(e.content)
