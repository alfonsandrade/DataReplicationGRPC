LEADER_PORT = 50051
REPLICA_PORTS = [50052, 50053, 50054]
REPLICA_ADDRESSES = [f"localhost:{port}" for port in REPLICA_PORTS]
