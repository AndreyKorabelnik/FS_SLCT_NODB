import socket
import os

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5001
BUFFER_SIZE = 4096


s = socket.socket()
s.bind((SERVER_HOST, SERVER_PORT))
# 5 here is the number of unaccepted connections that the system will allow before refusing new connections
s.listen(5)
print(f"[*] Listening as {SERVER_HOST}:{SERVER_PORT}")
client_socket, address = s.accept()
print(f"[+] {address} is connected.")

client_id = address[1]
client_directory = f'input/client_{client_id}'
os.mkdir(client_directory)

# receive using client socket, not server socket
filename = client_socket.recv(BUFFER_SIZE).decode()
# remove absolute path if there is
filename = os.path.basename(filename)
filename = f'{client_directory}/{filename}'
with open(filename, "wb") as f:
    while True:
        bytes_read = client_socket.recv(BUFFER_SIZE)
        if not bytes_read:
            break
        f.write(bytes_read)

# run selection
# send output files back to client
client_socket.close()
s.close()