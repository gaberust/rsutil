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

        self._running = False

    def run(self):
        self._running = True

        buffer = ""
        while self._running:
            data = self._server.recv(self.buffer_size)
            if not data:
                self._running = False
                break
            if self.encoding is None:
                buffer += data.decode()
            else:
                buffer += data.decode(self.encoding)

            while self.delimiter in buffer:
                message, delim, new_buffer = buffer.partition(self.delimiter)
                buffer = new_buffer
                self._put(message)

        self._server.close()

    def _put(self, message):
        if self.message_queue.full() and self.queue_overflow_remove_oldest:
            self.message_queue.get()
        self.message_queue.put(message)

    def get(self):
        return self.message_queue.get()

    def stop(self):
        self._running = True
