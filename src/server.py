import grpc
from concurrent import futures
import replic_pb2
import replic_pb2_grpc
import global_vars as gv

REPLICA_PORTS = [gv.REPLIC1_PORT]

class ReplicationServiceServicer(replic_pb2_grpc.ReplicationServiceServicer):
    def PushUpdate(self, request, context):
        print(f"Lider got update: {request.key} = {request.value}")
        with open("../databank/server.json", "a") as f:
            f.write(f"{request.key}:{request.value}\n")
        for port in REPLICA_PORTS:
            with grpc.insecure_channel(f'localhost:{port}') as channel:
                stub = replic_pb2_grpc.ReplicationServiceStub(channel)
                ack = stub.PushUpdate(request)
                print(f"Replica ack in port {port}: {ack.success}, {ack.message}")
        return replic_pb2.Ack(success=True, message="Update replicated to all replicas!")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replic_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicationServiceServicer(), server)
    server.add_insecure_port(F'[::]:{gv.SERVER_PORT}')
    server.start()
    print(F"gRPC server running in:{gv.SERVER_PORT}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
