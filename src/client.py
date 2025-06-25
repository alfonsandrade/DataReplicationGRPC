import grpc
import replic_pb2
import replic_pb2_grpc
import global_vars as gv

def run():
    with grpc.insecure_channel(f'localhost:{gv.SERVER_PORT}') as channel:
        stub = replic_pb2_grpc.ReplicationServiceStub(channel)
        response = stub.PushUpdate(replic_pb2.DataUpdate(key="foo", value="bar"))
        print(f"Server response: {response.message}")

if __name__ == '__main__':
    run()
