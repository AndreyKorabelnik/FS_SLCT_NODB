import requests
import uuid

url = 'http://127.0.0.1:5000/'


def download_result(session_id):
    response = requests.get(url + 'download', params={'session_id': session_id})
    if response.status_code == 200:
        with open('result/r1.zip', 'wb') as file:
            file.write(response.content)


def post_input(session_id, input_data_file, selection_file, universe_file):
    files = [('source', ('input_data.csv', open(input_data_file, 'rb'))),
             ('source', ('selection.json', open(selection_file, 'rb'))),
             ('source', ('universe.json', open(universe_file, 'rb')))]
    r = requests.post(url, params={'session_id': session_id}, files=files)


if __name__ == '__main__':
    session_id = str(uuid.uuid4())  # 'tteesstt'
    post_input(session_id,
               input_data_file='source_data/input_data_dax.csv',
               selection_file='source_data/selection.json',
               universe_file='source_data/universe.json')
    download_result(session_id)
