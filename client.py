import socket
import os
import argparse

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5001
BUFFER_SIZE = 4096


def send_file(filename, host, port):
    s = socket.socket()
    print(f"[+] Connecting to {host}:{port}")
    s.connect((host, port))
    print("[+] Connected.")

    # send the filename and filesize
    s.send(filename.encode())

    with open(filename, "rb") as f:
        while True:
            bytes_read = f.read(BUFFER_SIZE)
            if not bytes_read:
                break
            # we use sendall to assure transimission in busy networks
            s.sendall(bytes_read)
    s.close()



print(__name__)
filename = 'C:\AndreyK\mydev\Future State Selection Service\FS_SLCT_NODB\source_data\input_data.csv'
send_file(filename, SERVER_HOST, SERVER_PORT)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Simple File Sender")
#     parser.add_argument("file", help="File name to send")
#     parser.add_argument("host", help="The host/IP address of the receiver")
#     parser.add_argument("-p", "--port", help="Port to use, default is 5001", type=int, default=5001)
#     args = parser.parse_args()
#     filename = args.file
#     host = args.host
#     port = args.port
#     send_file(filename, host, port)