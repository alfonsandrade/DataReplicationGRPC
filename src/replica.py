import grpc
from concurrent import futures
import json
import os
import sys

import replic_pb2
import replic_pb2_grpc

class Replica(replic_pb2_grpc.ReplicationServiceServicer):
    def __init__(self, port):
        self.port = port
        self.log = []
        self.db = []
        self.load_files_locally()

    def load_files_locally(self):
        try:
            with open(f"../databank/replica_{self.port}_log.json", "r") as f:
                self.log = json.load(f)
            with open(f"../databank/replica_{self.port}_db.json", "r") as f:
                self.db = json.load(f)
        except FileNotFoundError:
            pass

    def save_log(self):
        with open(f"../databank/replica_{self.port}_log.json", "w") as f:
            json.dump(self.log, f, indent=4)

    def save_db(self):
        with open(f"../databank/replica_{self.port}_db.json", "w") as f:
            json.dump(self.db, f, indent=4)

    def AppendEntries(self, request, context):
        if request.leader_epoch < self.log[-1]['epoch'] if self.log else 0:
            print(f"AppendEntries: Ignoring request from leader with epoch {request.leader_epoch}")
            return replic_pb2.AppendEntriesResponse(success=False,
                                                    current_offset=self.log[-1]['offset'] if self.log else 0,
                                                    current_epoch=self.log[-1]['epoch'] if self.log else 0)

        if self.log and self.log[-1]['offset'] != request.prev_log_offset:
            self.log = [entry for entry in self.log if entry['offset'] <= request.prev_log_offset]
            self.save_log()
            print(f"AppendEntries: Log truncated to offset {request.prev_log_offset}")
            return replic_pb2.AppendEntriesResponse(success=False,
                                                    current_offset=self.log[-1]['offset'] if self.log else 0,
                                                    current_epoch=self.log[-1]['epoch'] if self.log else 0)

        entry = {
            'epoch': request.entry.epoch,
            'offset': request.entry.offset,
            'key': request.entry.key,
            'value': request.entry.value
        }
        self.log.append(entry)
        self.save_log()
        print(f"AppendEntries: Appended entry {entry['offset']} with key {entry['key']} and value {entry['value']}")
        return replic_pb2.AppendEntriesResponse(success=True,
                                                current_offset=entry['offset'],
                                                current_epoch=entry['epoch'])

    def CommitEntry(self, request, context):
        for entry in self.log:
            if entry['offset'] <= request.offset and entry['offset'] > self.db[-1]['offset'] if self.db else 0:
                self.db.append(entry)
        self.save_db()
        print(f"CommitEntry: Committed entries up to offset {request.offset}. Current DB size: {len(self.db)}")
        return replic_pb2.CommitResponse(success=True)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replic_pb2_grpc.add_ReplicationServiceServicer_to_server(Replica(port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Replica running on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python replica.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    serve(port)
