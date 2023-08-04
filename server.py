import os
import shutil
from flask import Flask, request, send_file
import general
import selection

app = Flask(__name__)


@app.route('/', methods=['POST'])
def upload():
    files = request.files.getlist("source")
    session_id = request.args.get('session_id')

    if files:
        client_input_folder = general.get_session_input_folder(session_id)
        client_output_folder = general.get_session_output_folder(session_id)
        general.make_dir(client_input_folder)
        for file in files:
            file.save(os.path.join(client_input_folder, file.filename))
        selection.run(client_input_folder, client_output_folder)
        return '', 200
    else:
        return None


@app.route('/download', methods=['GET'])
def download():
    session_id = request.args.get('session_id')
    client_output_folder = general.get_session_output_folder(session_id)
    filename = f'{client_output_folder}/all_outputs'
    shutil.make_archive(filename, 'zip', client_output_folder)
    return send_file(filename + '.zip', as_attachment=True)


Flask.run(app)
