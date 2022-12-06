from flask import Flask, request
from flask_cors import CORS, cross_origin
from haystack.pipelines import DocumentSearchPipeline
from utilities import *
import jsonpickle


app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
CORS(app)
reader_retriever = initialize_values()
# PIPELINE=ExtractiveQAPipeline(reader=reader_retriever[0], retriever=reader_retriever[1])

@app.route('/')

def hello_world():
    return "flask Dockerized"




@app.route('/ask', methods=['POST'])
@cross_origin()
def ask():
    if request.method == 'POST':
        search_pipe = DocumentSearchPipeline(reader_retriever)
        query = request.json['question']
        # pred = PIPELINE.run(query=q, params={"Retriever": {"top_k":10}, "Reader": {"top_k": 5}},debug=True)
        pred=search_pipe.run(query=query, params={"Retriever": {"top_k": 5}})

        # print("pred-------------",pred['documents'])

        return jsonpickle.encode(get_final_answers(pred['documents']))
        # return jsonpickle.encode(get_final_context(pred['documents']))



@app.route('/delete/blob-file', methods=['DELETE'])
def delete_blob_files():
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    my_content_settings = ContentSettings(content_type='application/pdf')
    entries = os.listdir('./highlighted-files')
    for e in entries:
        print("entryyy",e)
        container_client.delete_blob(blob=e)
        os.remove("./highlighted-files/" + e)
    return

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=True)