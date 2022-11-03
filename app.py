from flask import Flask, request
from flask_cors import CORS
from utilities import *
import jsonpickle


app = Flask(__name__)
CORS(app)


PIPELINE = initialize_values()


@app.route('/ask', methods=['POST'])
def ask():
    if request.method == 'POST':
        q = request.json['question']
        pred = PIPELINE.run(query=q, params={"Retriever": {"top_k":5}, "Reader": {"top_k": 5}})
        return jsonpickle.encode(get_final_answers(pred['answers']))


@app.route('/delete/blob-file', methods=['DELETE'])
def delete_blob_files():
    print("hellooooooooooooooooo");
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    my_content_settings = ContentSettings(content_type='application/pdf')
    entries = os.listdir('./highlighted-files')

    for e in entries:
        print("entryyy",e)
        container_client.delete_blob(blob=e)
        os.remove("./highlighted-files/" + e)
    return


if __name__ == '__main__':
    app.run()