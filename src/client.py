import grpc
import replic_pb2
import replic_pb2_grpc
import global_vars as gv

def run():
    while True:
        command = input("Enter command (write <key> <value> or query <key>): ")
        parts = command.split()
        if not parts:
            continue

        cmd = parts[0]
        with grpc.insecure_channel(f'localhost:{gv.LEADER_PORT}') as channel:
            stub = replic_pb2_grpc.ClientServiceStub(channel)
            if cmd == "write" and len(parts) == 3:
                response = stub.Write(replic_pb2.WriteRequest(key=parts[1], value=parts[2]))
                print(f"Leader response: {response.message}")
            elif cmd == "query" and len(parts) == 2:
                response = stub.Query(replic_pb2.QueryRequest(key=parts[1]))
                if response.found:
                    print(f"Value: {response.value}")
                else:
                    print("Key not found.")
            else:
                print("Invalid command.")

if __name__ == '__main__':
    run()
