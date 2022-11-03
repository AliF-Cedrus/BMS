from haystack.nodes import TextConverter, PDFToTextConverter, DocxToTextConverter, PreProcessor
# from io import BytesIO
#
# connection_string="DefaultEndpointsProtocol=https;AccountName=bmsblobpoc;AccountKey=Pcsg/q2UjCqb+2R6VsSEHdotKWvq0NjIXMTmNMXs6Az9CjsoFEI8olBsiQYTrLHpdgriOJq+RlqB+AStnN/EJA==;EndpointSuffix=core.windows.net"
# container_name='bmspoc'
# # account_url = "https://bmsblobpoc.blob.core.windows.net"
#
#
# import fitz
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
# import re
#
# # def search_for_text(lines, search_str):
# #     """
# #     Search for the search string within the document lines
# #     """
# #     for line in lines:
# #         # Find all matches within one line
# #         results = re.findall(search_str, line, re.IGNORECASE)
# #         # In case multiple matches within one line
# #         for result in results:
# #             yield result
# #
# #
# # text="""\nWhat is ABECMA?\nABECMA is for the treatment of multiple myeloma in patients who have received at least four kinds\nof treatment regimens that have not worked or have stopped working  ABECMA is a medicine\nmade from your own white blood cells; the cells are genetically modified to recognize and attack\nyour multiple myeloma cells"""
# #
# # def highlight_matching_data(page, matched_values):
# #     """
# #     Highlight matching values
# #     """
# #     matches_found = 0
# #     # Loop throughout matching values
# #     for val in matched_values:
# #         matches_found += 1
# #         matching_val_area = page.searchFor(val)
# #         # print("matching_val_area",matching_val_area)
# #         highlight = page.addHighlightAnnot(matching_val_area)
# #
# #     return matches_found
# #
# #
# #
# #
# # def process_data(input_file: str, output_file: str, search_str: str):
# #     """
# #     Process the pages of the PDF File
# #     """
# #     # Open the PDF
# #     pdfDoc = fitz.open(input_file)
# #     # Save the generated PDF to memory buffer
# #     output_buffer = BytesIO()
# #     total_matches = 0
# #     # Iterate through pages
# #     for pg in range(pdfDoc.pageCount):
# #         # Select the page
# #         page = pdfDoc[pg]
# #         # Get Matching Data
# #         # Split page by lines
# #         page_lines = page.getText("text").split('\n')
# #         matched_values = search_for_text(page_lines, search_str)
# #         if matched_values:
# #                 matches_found = highlight_matching_data(page, matched_values, 'Highlight')
# #
# #          # Save to output
# #     pdfDoc.save(output_buffer)
# #     pdfDoc.close()
# #     # Save the output buffer to the output file
# #     with open(output_file, mode='wb') as f:
# #         f.write(output_buffer.getbuffer())
# #
# #
# #
# # process_data("./medguide_abecma.pdf","testingg.pdf",text)
#
#
#
# from utilities import *
#
#
# # text="""\nWhat is ABECMA? \nABECMA is for the treatment of multiple myeloma in patients who have received at least four kinds\nof treatment regimens that have not worked or have stopped working  ABECMA is a medicine\nmade from your own white blood cells; the cells are genetically modified to recognize and attack\nyour multiple myeloma cells"""
# # text=""" What is ABECMA?
# # ABECMA is for the treatment of multiple myeloma in patients who have received at least four kinds
# # of treatment regimens that have not worked or have stopped working. ABECMA is a medicine
# # made from your own white blood cells; the cells are genetically modified to recognize and attack
# # your multiple myeloma cells."""
# # print(doc.load_page(0).get_text())
# # for page in doc:
# #    print(page.get_text())
#
# # text="""\nWhat is ABECMA?\nABECMA is for the treatment of multiple myeloma in patients who have received at least four kinds\nof treatment regimens that have not worked or have stopped working  ABECMA is a medicine\nmade from your own white blood cells; the cells are genetically modified to recognize and attack\nyour multiple myeloma cells"""
# #
# # list=text.split('\n')
# # list.remove('')
# # print("list",list)
# # text='ABECMA is for the treatment of multiple myeloma in patients who have received at least four kind'
#
# doc = fitz.open("medguide_abecma.pdf")
# def highlight(text,doc,doc_name_saved):
#
#     url="https://bmsblobpoc.blob.core.windows.net/bmspoc/"
#     for page in doc:
#         print(page.get_text("text"))
#         # for text in list:
#         text_instances = page.search_for(text)
#         print("text instances :",text_instances)
#         for inst in text_instances:
#                 print(True)
#                 highlight = page.add_highlight_annot(inst)
#                 highlight.update()
#
#     doc_name_saved=doc_name_saved+".pdf"
#     doc.save(doc_name_saved, garbage=4, deflate=True, clean=True)
#
#     try:
#         container_client = ContainerClient.from_connection_string(connection_string, container_name)
#         my_content_settings = ContentSettings(content_type='application/pdf')
#         blob_client = container_client.get_blob_client(doc_name_saved)
#         with open(file=doc_name_saved,mode='rb') as data:
#             blob_client.upload_blob(data, overwrite=True, content_settings=my_content_settings)
#
#     except Exception as ex:
#         print('Exception:')
#         print(ex)
#
#     return [url+doc_name_saved]
#
#
#
# text="severe nausea, vomiting, diarrhea"
#
#
# print(highlight(text,doc,"alif3"))
#


# def split_word_into_paragraphs(word, length):
#     list_of_sentences = []
#     # text = re.sub(r'\n\n\n+','newP',word)
#     # text = re.sub(r'[\n\uf0b7]','',text)
#     # text=re.sub('o•','',text)
#     # splitted_sentences=text.split('newP')
#     text = word.split('. ')
#     for i in range(len(text)):
#         list_of_sentences.append(text[i])
#
#     return list_of_sentences


# def combine_sentences(sentences, number):
#     index_sentence = 0
#     k = 1
#
#     while index_sentence < len(sentences) - k:
#         if (len(sentences[index_sentence]) + len(sentences[index_sentence + 1]) <= number):
#             sentences[index_sentence] = sentences[index_sentence] + " " + sentences[index_sentence + 1]
#             sentences.remove(sentences[index_sentence + 1])
#             k += 1
#         index_sentence += 1
#     return sentences
#

# converter = PDFToTextConverter(remove_numeric_tables=True, valid_languages=["en"])
# doc_pdf = converter.convert(file_path="medguide_abecma.pdf")[0]
# sentences =split_word_into_paragraphs(doc_pdf.content,300)
# old_size=len(sentences)
#
# print(len(sentences))
# for e in sentences:
#     new_list=[]
#     print("hello")
#     if(len(e)>500):
#         new_list=e.split(', ')
#         sentences.remove(e)
#
#     for l in new_list:
#         sentences.append(l+', ')
#
# while True:
#       sentences=combine_sentences(sentences,300)
#       new_size=len(sentences)
#       if(new_size==old_size):
#           break
#       else:
#           old_size=len(sentences)
#
#
# print(len(sentences))
#
# for e in sentences:
#     print(e,'=>>>> with length',len(e))
#     print("-----------------")
#
# print("list_of_paraph",sentences)