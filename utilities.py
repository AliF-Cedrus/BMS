from haystack.document_stores import FAISSDocumentStore
from haystack.nodes import EmbeddingRetriever,FARMReader
from haystack.pipelines import ExtractiveQAPipeline
import PyPDF2
import fitz
import re
from azure.storage.blob import ContainerClient, ContentSettings
import random
import os


os.environ["TOKENIZERS_PARALLELISM"] = "false"
connection_string = "DefaultEndpointsProtocol=https;AccountName=bmsblobpoc;AccountKey=Pcsg/q2UjCqb+2R6VsSEHdotKWvq0NjIXMTmNMXs6Az9CjsoFEI8olBsiQYTrLHpdgriOJq+RlqB+AStnN/EJA==;EndpointSuffix=core.windows.net"
container_name = 'bmspoc'


def credentials():
    connectionstring = "DefaultEndpointsProtocol=https;AccountName=bmsblobpoc;AccountKey=Pcsg/q2UjCqb+2R6VsSEHdotKWvq0NjIXMTmNMXs6Az9CjsoFEI8olBsiQYTrLHpdgriOJq+RlqB+AStnN/EJA==;EndpointSuffix=core.windows.net"
    container = 'bmspoc'
    return [connectionstring, container]


##we should split the text into small sentences composed of number of words = num_words
def extract_sentence_from_context(text,num_words):
    list_text=text.split(' ')
    list_of_sentences = []
    for i in range(len(list_text)-num_words):
        list_of_sentences.append(" ".join(list_text[i:i+num_words]))
    return list_of_sentences


##return word location by page
def word_search(key,extracted_text):
    for page in range(len(extracted_text)):
      for line_number, line in enumerate(extracted_text[page], 1):  # using enumerate to map each line of the file to it's line_number
        if key in line:  # searching for the keyword in file
            return page + 1


def extract_text_from_pdf(pdf_file: str) -> [str]:
    # Open the PDF file of your choice
    doc_dir = "./documents/"
    with open(doc_dir+pdf_file, 'rb') as pdf:
        reader = PyPDF2.PdfFileReader(pdf, strict=True)
        pdf_text = []
        for page in reader.pages:
            content = page.extract_text()
            pdf_text.append(content.split(' \n'))

        return pdf_text


def initialize_values():
    print("loading doc_store-------")
    document_store = FAISSDocumentStore.load("bms")


    print("loading retriever-------")

    Retriever = EmbeddingRetriever(
        document_store=document_store,
        embedding_model="sentence-transformers/all-mpnet-base-v2",
        model_format="sentence_transformers",
    )


    print("loaded Retrieverrrrrrrrrrrrr -------",Retriever)



    print("loading Readerrrrrrrrrrrrrrrrrr-------")


    # Reader = FARMReader(model_name_or_path='deepset/roberta-base-squad2',
    #                     context_window_size=1500,
    #                     max_seq_len=512,
    #                     return_no_answer=True,
    #                     no_ans_boost=0,
    #                     use_gpu=False)


    # print("loaded Readerrrrrrrrrrrrrrrrrr-------",Reader)

    #
    Reader = FARMReader(model_name_or_path="ahotrod/albert_xxlargev1_squad2_512",
                        context_window_size=500,
                        max_seq_len=512,
                        return_no_answer=True,
                        no_ans_boost=0,
                        use_gpu=False)


    print("loaded Readerrrrrrrrrrrrrrrrrr-------",Reader)



    return [Reader,Retriever]

def highlight(list_of_short_answers):
   print("liotttt of short ansdwrrrs",list_of_short_answers)
   list_of_url = {}
   url = "https://bmsblobpoc.blob.core.windows.net/bmspoc/"

   for doc_name in list_of_short_answers.keys():
        doc = fitz.open("./documents/"+doc_name)
        for page in doc:
            for answers in list_of_short_answers[doc_name].split('/'):
                text = answers.split('\f')
                for sentences in text:
                    text_instances = page.search_for(sentences)
                    if text_instances:
                      for inst in text_instances:
                        highlight = page.add_highlight_annot(inst)
                        highlight.update()

        doc_name_saved = str(random.randint(0, 20000))
        doc_name_saved = doc_name_saved + ".pdf"
        doc.save('./highlighted-files/' + doc_name_saved, garbage=4, deflate=True, clean=True)

        try:
            container_client = ContainerClient.from_connection_string(connection_string, container_name)
            my_content_settings = ContentSettings(content_type='application/pdf')
            blob_client = container_client.get_blob_client(doc_name_saved)
            with open(file='./highlighted-files/'+ doc_name_saved,mode='rb') as data:
                    blob_client.upload_blob(data, overwrite=True, content_settings=my_content_settings)

        except Exception as ex:
            print('Exception:')
            print(ex)

        list_of_url[doc_name]=url +doc_name_saved

   return list_of_url


def list_of_short_answer(answers):
    list_of_short_answers = {}
    list_of_short_context = {}
    meta_data = []
    for info in answers:
        for meta_val in info.meta.values():
            meta_data.append(meta_val)

        meta_data = meta_data[:-1]
        start = info.offsets_in_document[0].start
        end = info.offsets_in_document[0].end

        if start == 0 and end == 0:
            continue
        else:
            if meta_data[0] in list_of_short_answers.keys():
                list_of_short_answers[meta_data[0]] = list_of_short_answers[meta_data[0]] + '/' + str(info.answer)
                list_of_short_context[meta_data[0]] = list_of_short_context[meta_data[0]] + '/' + str(info.context)
            else:
                list_of_short_answers[meta_data[0]] = info.answer
                list_of_short_context[meta_data[0]] = info.context

    return highlight(list_of_short_context)


def get_final_answers(answers):
    url_to_doc_mapping = list_of_short_answer(answers)
    answer_info = []

    for info in answers:
        meta_data = []

        for meta_val in info.meta.values():
            meta_data.append(meta_val)

        if meta_data:
            meta_data = meta_data[:-1]
            doc_name = meta_data[0]
            extracted_text = extract_text_from_pdf(doc_name)

        start = info.offsets_in_document[0].start
        end = info.offsets_in_document[0].end

        if start == 0 and end == 0:
            continue

        else:
            context = info.context
            list_sentences = extract_sentence_from_context(context, 4)
            possible_pages = []
            for sentence in list_sentences:
                page_occurence = word_search(sentence,extracted_text)
                if page_occurence is not None:
                    possible_pages.append(page_occurence)
                else:
                    continue

            if possible_pages and doc_name in url_to_doc_mapping:
                max_occurence_of_page = max(possible_pages, key=possible_pages.count)
                answer_info.append({"answer": info.answer, "context": re.sub('\\n',' ',context),"url_highlighted": url_to_doc_mapping[doc_name] + "#page=" + str(max_occurence_of_page), "meta_data": meta_data, "page": max_occurence_of_page})

    return answer_info