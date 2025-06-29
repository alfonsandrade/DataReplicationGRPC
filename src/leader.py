import grpc
from concurrent import futures
import time
import json
import threading

import replic_pb2
import replic_pb2_grpc
import global_vars as gv

class Leader(replic_pb2_grpc.ClientServiceServicer):
    def __init__(self):
        self.log = []
        self.db = []
        self.epoch = 0
        self.offset = 0
        self.lock = threading.Lock()
        self.load_files_locally()

    def load_files_locally(self):
        try:
            with open("../databank/leader_log.json", "r") as f:
                self.log = json.load(f)
                if self.log:
                    self.offset = self.log[-1]['offset']
                    self.epoch = self.log[-1]['epoch']
            with open(f"../databank/leader_db.json", "r") as f:
                self.db = json.load(f)
        except FileNotFoundError:
            pass

    def save_log(self):
        with open("../databank/leader_log.json", "w") as f:
            json.dump(self.log, f, indent=4)

    def save_db(self):
        with open(f"../databank/leader_db.json", "w") as f:
            json.dump(self.db, f, indent=4)

    def append_missing_entries_after_offset(self, replica_address, stub: replic_pb2_grpc.ReplicationServiceStub, offset):
        try:
            for entry in self.log:
                if entry['offset'] > offset:
                    response = stub.AppendEntries(
                        replic_pb2.AppendEntriesRequest(
                            leader_epoch=self.epoch,
                            prev_log_offset=offset,
                            entry=replic_pb2.LogEntry(
                                epoch=entry['epoch'],
                                offset=entry['offset'],
                                key=entry['key'],
                                value=entry['value']
                            )
                        )
                    )
                    if response.success:
                        print(f"APPEND_MISSING: Appended entry {entry['offset']} to replica {replica_address}.")
                        offset = entry['offset']
                    else:
                        print(f"APPEND_MISSING: Failed to append entry {entry['offset']} to replica.")
                        return 0
        except grpc.RpcError as e:
            print(f"APPEND_MISSING: Error replicating to {replica_address}: {e}")
            return 0
        
        return 1

    def Write(self, request, context):
        with self.lock:
            self.offset += 1
            entry = {
                'epoch': self.epoch,
                'offset': self.offset,
                'key': request.key,
                'value': request.value
            }
            self.log.append(entry)
            self.save_log()

            acks = 0
            for replica_address in gv.REPLICA_ADDRESSES:
                try:
                    with grpc.insecure_channel(replica_address) as channel:
                        stub = replic_pb2_grpc.ReplicationServiceStub(channel)
                        response = stub.AppendEntries(
                                                    replic_pb2.AppendEntriesRequest(
                                                        leader_epoch=self.epoch,
                                                        prev_log_offset=self.offset - 1,
                                                        entry=replic_pb2.LogEntry(
                                                            epoch=entry['epoch'],
                                                            offset=entry['offset'],
                                                            key=entry['key'],
                                                            value=entry['value']
                                                        )
                                                    )
                                        )
                        if response.success:
                            acks += 1
                        else:
                            acks += self.append_missing_entries_after_offset(replica_address, stub, response.current_offset)
                except grpc.RpcError as e:
                    print(f"Error replicating to {replica_address}: {e}")

            if acks >= len(gv.REPLICA_ADDRESSES) // 2 + 1:
                for replica_address in gv.REPLICA_ADDRESSES:
                    try:
                        with grpc.insecure_channel(replica_address) as channel:
                            stub = replic_pb2_grpc.ReplicationServiceStub(channel)
                            stub.CommitEntry(replic_pb2.CommitRequest(offset=self.offset))
                    except grpc.RpcError as e:
                        print(f"Error committing to {replica_address}: {e}")

                self.db.append(entry)
                self.save_db()
                return replic_pb2.WriteResponse(success=True, message="Data committed.")
            else:
                return replic_pb2.WriteResponse(success=False, message="Failed to get quorum.")

    def Query(self, request, context):
        with self.lock:
            for entry in self.db:
                if entry['key'] == request.key:
                    return replic_pb2.QueryResponse(value=entry['value'], found=True)
            return replic_pb2.QueryResponse(found=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replic_pb2_grpc.add_ClientServiceServicer_to_server(Leader(), server)
    server.add_insecure_port(f'[::]:{gv.LEADER_PORT}')
    server.start()
    print(f"Leader running on port {gv.LEADER_PORT}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
