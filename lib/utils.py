import json
import sys
import struct


class Formatter:

    def __init__(self, headers, data):
        self.headers = headers
        self.data = data
        self.formatted_data = list()

    def print_data(self):
        if isinstance(self.formatted_data[0], bytearray):
            for data in self.formatted_data:
                sys.stdout.buffer.write(data)
        elif isinstance(self.formatted_data[0], str):
            for data in self.formatted_data:
                print(data)

    def format_data(self, _type):
        if _type == 'json':
            self._json_form()
        elif _type == 'tsv':
            self._tsv_form()
        elif _type == 'binary':
            self._binary_form()
        else:
            raise ValueError("Unsupported formatting type!")

    def filter_data(self, required_columns):
        positions = list()
        new_headers = list()
        new_data = list()
        for column in required_columns:
            try:
                positions.append(self.headers.index(column))
                new_headers.append(column)
            except ValueError:
                raise ValueError("Unknown column.")

        self.headers = new_headers
        for row in self.data:
            new_data.append([row[i] for i in positions])
        self.data = new_data

    def _json_form(self):
        self.formatted_data = [json.dumps(dict(zip(self.headers, row)))
                               for row in self.data]

    def _tsv_form(self):
        self.formatted_data.append('\t'.join(self.headers))
        for row in self.data:
            self.formatted_data.append('\t'.join(map(str, row)))

    def _binary_form(self):
        for row in [self.headers] + self.data:
            line = bytearray()
            for field in row:
                header_enc = str(field).encode('utf8')
                buf = struct.pack('>I', len(header_enc))
                line = bytearray(line + buf + header_enc)
            self.formatted_data.append(line + bytearray([0x0a]))
