import grpc
import replic_pb2
import replic_pb2_grpc
import global_vars as gv

def run():
    while True:
        command = input("Enter command (write <key> <value> <testError> or query <key>): ")
        cmd_inputs = command.split()
        if not cmd_inputs:
            continue

        cmd = cmd_inputs[0]
        with grpc.insecure_channel(f'localhost:{gv.LEADER_PORT}') as channel:
            stub = replic_pb2_grpc.ClientServiceStub(channel)
            if cmd == "write" and len(cmd_inputs) >= 3:
                response = stub.Query(replic_pb2.QueryRequest(key=cmd_inputs[1]))
                if response.found:
                    print("Key already exists. Use 'query' to check value.")
                else:
                    print(cmd_inputs)
                    if len(cmd_inputs) == 4:
                        response = stub.Write(replic_pb2.WriteRequest(key=cmd_inputs[1], value=cmd_inputs[2], testError=cmd_inputs[3]))
                    else:
                        response = stub.Write(replic_pb2.WriteRequest(key=cmd_inputs[1], value=cmd_inputs[2], testError="0"))
                    print(f"Leader response: {response.message}")
            elif cmd == "query" and len(cmd_inputs) == 2:
                response = stub.Query(replic_pb2.QueryRequest(key=cmd_inputs[1]))
                if response.found:
                    print(f"Value: {response.value}")
                else:
                    print("Key not found.")
            else:
                print("Invalid command.")

if __name__ == '__main__':
    run()
