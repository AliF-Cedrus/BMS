from haystack.nodes import TextConverter, PDFToTextConverter, DocxToTextConverter, PreProcessor
import fitz
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
import re


connection_string = "DefaultEndpointsProtocol=https;AccountName=bmsblobpoc;AccountKey=Pcsg/q2UjCqb+2R6VsSEHdotKWvq0NjIXMTmNMXs6Az9CjsoFEI8olBsiQYTrLHpdgriOJq+RlqB+AStnN/EJA==;EndpointSuffix=core.windows.net"
container_name ='bmspoc'


def highlight(text, doc, doc_name_saved):
    url = "https://bmsblobpoc.blob.core.windows.net/bmspoc/"
    for page in doc:
        print(page.get_text("text"))
        # for text in list:
        text_instances = page.search_for(text)
        print("text instances :",text_instances)
        for inst in text_instances:
            print(True)
            highlight = page.add_highlight_annot(inst)
            highlight.update()

    doc_name_saved=doc_name_saved+".pdf"
    doc.save("./highlighted-files/" + doc_name_saved, garbage=4, deflate=True, clean=True)

    try:
        container_client = ContainerClient.from_connection_string(connection_string, container_name)
        my_content_settings = ContentSettings(content_type='application/pdf')
        blob_client = container_client.get_blob_client(doc_name_saved)
        with open(file="./highlighted-files/"+doc_name_saved,mode='rb') as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=my_content_settings)

    except Exception as ex:
        print('Exception:')
        print(ex)

    return [url+doc_name_saved]


text = """Reports of lactic acidosis with BARACLUDE generally involved\npatients who were seriously ill due to their liver disease or other medical condition."""
print("textttt", text)
doc = fitz.open("../documents/ppi_baraclude.pdf")

print(highlight(text,doc,"ali_3"))