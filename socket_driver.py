# ChatGPT Link: https://chatgpt.com/share/fc3301ea-000e-4c17-b5e7-6e0681f91c61.

import socket
import bson
import struct


class MongoClient:
    def __init__(self, host='localhost', port=27017):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def send_command(self, command, database='admin'):
        # Add $db field to the command
        command["$db"] = database

        # Serialize the command to BSON
        request_id = 1
        response_to = 0
        op_code = 2013  # OP_MSG operation code
        flags = 0
        command_bson = bson.BSON.encode(command)

        # OP_MSG message format with a single section kind 0
        section_kind = 0
        section = struct.pack('<B', section_kind) + command_bson

        # Message header + flags + section
        message = (
                struct.pack('<iiii', 16 + 4 + len(section), request_id, response_to, op_code) +  # Header
                struct.pack('<i', flags) +  # Flags
                section  # Section (section kind + command)
        )

        # Send the command to the MongoDB server
        self.socket.sendall(message)

        # Read the response header
        response_header = self._recv_all(21)
        total_length, response_id, response_to, op_code, flags, section_kind = struct.unpack('<iiiiiB', response_header)

        # Read the full response
        response_body = self._recv_all(total_length - len(response_header))

        # Decode the response
        response = bson.BSON(response_body).decode()

        return response

    def _recv_all(self, length):
        """ Helper function to receive exactly `length` bytes from the socket. """
        data = b""
        while len(data) < length:
            packet = self.socket.recv(length - len(data))
            if not packet:
                raise ConnectionError("Socket connection broken")
            data += packet
        return data


    def close(self):
        self.socket.close()


# Example usage
if __name__ == "__main__":
    client = MongoClient()
    # command = {"ping": 1}
    # command = {"listDatabases": 1}
    # command = {"listCollections": 1, "filter": {}, "nameOnly": True}
    # command = {
    #     "find": "get_user",  # collection name
    #     "filter": {},
    #     "batchSize": 10  # Adjust batch size as needed
    # }
    command = {
        "find": "get_user",  # collection name
        "filter": {"user_id": 1},  # filter criteria
        "limit": 1
    }
    response = client.send_command(command, database="testing")
    print(response)
    client.close()
