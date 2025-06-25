import grpc
from concurrent import futures
import replic_pb2
import replic_pb2_grpc

import global_vars as gv

class ReplicationServiceServicer(replic_pb2_grpc.ReplicationServiceServicer):
    def PushUpdate(self, request, context):
        print(f"Replica got update: {request.key} = {request.value}")
        with open(f"../databank/replic_{gv.REPLIC1_PORT}.json", "a") as f:
            f.write(f"{request.key}:{request.value}\n")
        return replic_pb2.Ack(success=True, message="Replica updated!")

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replic_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicationServiceServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Replica running in port :{port}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve(gv.REPLIC1_PORT)
