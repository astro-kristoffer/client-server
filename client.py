import socket
import time


class ClientError(Exception):
    pass


class Client:

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        try:
            self.sock = socket.create_connection((host,port),timeout)
        except socket.error as err:
            raise ClientError('',err)

    def answer(self):
        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.sock.recv(1024)
            except socket.error as err:
                raise ClientError('',err)

        str_data = data.decode('utf8')

        status, payload = str_data.split("\n", 1)
        payload = payload.strip()

        if status == "error":
            raise ClientError(payload)

        return payload

    def put(self, key, value, timestamp=None):
        timestamp = timestamp or str(int(time.time()))
        try:
            self.sock.sendall(f'put {key} {value} {timestamp}\n'.encode('utf8'))
        except socket.error as err:
            raise ClientError('',err)
        self.answer()

    def get(self, key):
        try:
            self.sock.sendall(f"get {key}\n".encode('utf8'))
        except socket.error as err:
            raise ClientError('',err)
        payload = self.answer()
        data = {}
        if payload == "":
            return data
        for row in payload.split("\n"):
            key, value, timestamp = row.split()
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data

    def close(self):
        try:
            self.sock.close()
        except socket.error as err:
            raise ClientError('',err)


if __name__ == '__main__':

    client = Client("127.0.0.1", 8888, timeout=15)

    client.put("palm.cpu", 0.5, timestamp=1150864247)
    client.put("palm.cpu", 2.0, timestamp=1150864248)
    client.put("palm.cpu", 0.5, timestamp=1150864248)

    client.put("eardrum.cpu", 3, timestamp=1150864250)
    client.put("eardrum.cpu", 4, timestamp=1150864251)
    client.put("eardrum.memory", 4200000)

    print(client.get("*"))
    client.close()