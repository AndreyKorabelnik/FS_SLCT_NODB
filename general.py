import os
import shutil

input_folder = 'input'
output_folder = 'output'


def get_client_id():
    return 1


def get_client_input_folder(client_id):
    return os.path.join(input_folder, f'client_{client_id}')


def get_client_output_folder(client_id):
    return os.path.join(output_folder, f'client_{client_id}')


def make_dir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)