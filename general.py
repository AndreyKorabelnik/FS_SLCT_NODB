import os
import shutil

input_folder = 'input'
output_folder = 'output'


def get_session_input_folder(session_id):
    return os.path.join(input_folder, f'session_{session_id}')


def get_session_output_folder(session_id):
    return os.path.join(output_folder, f'session_{session_id}')


def make_dir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)