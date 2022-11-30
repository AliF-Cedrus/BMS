from haystack.nodes import PDFToTextConverter, FARMReader
from haystack.nodes import EmbeddingRetriever
from haystack.document_stores import FAISSDocumentStore
document_store = FAISSDocumentStore(faiss_index_factory_str="Flat", similarity='cosine')
doc_dir = './documents/'

def split_word_into_paragraphs(word):
    list_of_sentences = []
    text = word.split('. ')
    for i in range(len(text)):
        list_of_sentences.append(text[i])
    return list_of_sentences


def combine_sentences(sentences, number):
    index_sentence = 0
    k = 1

    while index_sentence < len(sentences)-k:
        if (len(sentences[index_sentence]) + len(sentences[index_sentence+1]) <= number) :
            sentences[index_sentence]=sentences[index_sentence]+sentences[index_sentence+1]
            sentences.remove(sentences[index_sentence+1])
            k += 1
        index_sentence += 1
    return sentences

list_documents=[
('medguide_abecma.pdf','https://packageinserts.bms.com/medguide/medguide_abecma.pdf'),('ppi_abraxane.pdf','https://www.bms.com/patient-and-caregivers/our-medicines.html'),
('medguide_zeposia.pdf', 'https://packageinserts.bms.com/medguide/medguide_yervoy.pdf'),
('ppi_abraxane.pdf','https://www.bms.com/patient-and-caregivers/our-medicines.html'),
('medguide_yervoy.pdf','https://packageinserts.bms.com/medguide/medguide_yervoy.pdf' ),
('medguide_thalomid.pdf', 'https://packageinserts.bms.com/medguide/medguide_thalomid.pdf'),
('ppi_evotaz.pdf','https://packageinserts.bms.com/ppi/ppi_evotaz.pdf'),
('ppi_empliciti.pdf','https://packageinserts.bms.com/ppi/ppi_empliciti.pdf'),
('medguide_eliquis.pdf','https://packageinserts.bms.com/medguide/medguide_eliquis.pdf'),
('medguide_droxia.pdf','https://packageinserts.bms.com/medguide/medguide_droxia.pdf'),
('ppi_baraclude.pdf','https://packageinserts.bms.com/ppi/ppi_baraclude.pdf'),
('medguide_camzyos.pdf','https://packageinserts.bms.com/medguide/medguide_camzyos.pdf')
]

list_of_paragraphs = []
for e in list_documents:
   converter = PDFToTextConverter(remove_numeric_tables=True, valid_languages=["en"])
   doc_pdf = converter.convert(file_path=doc_dir + e[0])[0]
   sentences = split_word_into_paragraphs(doc_pdf.content)
   old_size = len(sentences)
   new_sentences = []

   for s in sentences:
       new_list = []


       if len(s) >= 450:
           new_list = s.split(', ')
           for l in new_list:
                new_sentences.append(l + ', ')
       else:
           new_sentences.append((s))

   sentences = new_sentences

   while True:
      sentences = combine_sentences(sentences,450)
      new_size = len(sentences)

      if new_size == old_size:
          break      
      else:
          old_size = len(sentences)

   for S in sentences:
       print(S)
       print(len(S))

   list_of_paragraphs.append([sentences,e[0],e[1]])

print("list_of_paraph",list_of_paragraphs)

for e in list_of_paragraphs:
    print('len',len(e[0]))

dicts = []

for e in list_of_paragraphs:
    for s in e[0]:
      obj = {"content": s, "meta": {'doc': e[1],'link': e[2]}}
      dicts.append(obj)

document_store.write_documents(dicts)


print("loading retriver------")
retriever = EmbeddingRetriever(
    document_store=document_store,
    embedding_model="sentence-transformers/all-mpnet-base-v2",
    model_format="sentence_transformers",
)


# print("loading readerrrrrrrrrrrrrrr------")
#
# reader = FARMReader(model_name_or_path='deepset/roberta-base-squad2',
#                     context_window_size=1500,
#                     max_seq_len=500,
#                     return_no_answer=True,
#                     no_ans_boost=0,
#                     use_gpu=False)

document_store.update_embeddings(
    retriever,
    batch_size=100
)

document_store.save("bms")