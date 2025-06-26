import subprocess
import platform
import os
import sys
import time

import global_vars as gv

def run_script_in_new_terminal(script, args=[]):
    system = platform.system()
    command = [sys.executable, script] + args

    if system == "Windows":
        subprocess.Popen(["start", "cmd", "/k"] + command, shell=True)
    elif system == "Linux":
        try:
            subprocess.Popen(["x-terminal-emulator", "-e"] + command)
        except FileNotFoundError:
            try:
                subprocess.Popen(["gnome-terminal", "--"] + command)
            except FileNotFoundError:
                print("Could not find a compatible terminal emulator.")
    elif system == "Darwin":
        subprocess.Popen(["open", "-a", "Terminal", os.path.abspath(script)] + args)
    else:
        print(f"Unsupported platform: {system}")

if __name__ == "__main__":
    # Start the leader
    run_script_in_new_terminal("leader.py")
    time.sleep(1)

    # Start the replicas
    for port in gv.REPLICA_PORTS:
        run_script_in_new_terminal("replica.py", [str(port)])
        time.sleep(0.5)

    # Start the client
    run_script_in_new_terminal("client.py")
