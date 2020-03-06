#!/usr/bin/env python3

import threading
import queue
import socket


class TCPMessageQueue(threading.Thread):

    def __init__(self, host, port,
                 delimiter='\n', buffer_size=256,
                 max_queue_size=1024, queue_overflow_remove_oldest=False,
                 encoding=None):
        threading.Thread.__init__(self)

        self.host = host
        self.port = port
        self.delimiter = delimiter
        self.buffer_size = buffer_size
        self.max_queue_size = max_queue_size
        self.queue_overflow_remove_oldest = queue_overflow_remove_oldest
        self.encoding = encoding

        self.message_queue = queue.Queue(max_queue_size)

        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.connect((host, port))

    def run(self):
        length = None
        buffer = ""

        while True:
            if self.encoding is None:
                data = self._server.recv(self.buffer_size)
            else:
                data = self._server.recv(self.buffer_size).decode(self.encoding)
            if not data:
                break
            buffer += data

            while True:
                if length is None:
                    if self.delimiter not in buffer:
                        break
                    len_str, delimiter, buffer = buffer.partition(self.delimiter)
                    length = int(len_str)

                if len(buffer) < length:
                    break

                message = buffer[:length]
                buffer = buffer[length:]
                length = None

                if self.queue_overflow_remove_oldest and self.message_queue.full():
                    self.message_queue.get()
                self.message_queue.put(message)

    def get(self):
        return self.queue.get()

    def close(self):
        self._server.close()

