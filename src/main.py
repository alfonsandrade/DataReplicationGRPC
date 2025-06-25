import subprocess
import platform
import os
import sys

def run_script_in_current_terminal(script):
    subprocess.run([sys.executable, script])

def run_script_in_new_terminal(script):
    system = platform.system()

    if system == "Windows":
        # Use cmd.exe to open a new window and run the script
        subprocess.Popen(["start", "cmd", "/k", f"{sys.executable} {script}"], shell=True)

    elif system == "Linux":
        # Linux is the only operating system that has a variety of terminal emulators
        terminal_cmds = [
            ["gnome-terminal", "--", sys.executable, script],
            ["x-terminal-emulator", "-e", f"{sys.executable} {script}"],
            ["xterm", "-e", f"{sys.executable} {script}"],
            ["konsole", "-e", sys.executable, script]
        ]
        for cmd in terminal_cmds:
            try:
                subprocess.Popen(cmd)
                break
            except FileNotFoundError:
                continue
        else:
            print("⚠️ No compatible terminal emulator found.")

    elif system == "Darwin":  # macOS
        # Use AppleScript to open Terminal and run the script
        subprocess.Popen([
            "osascript", "-e",
            f'tell application "Terminal" to do script "{sys.executable} {os.path.abspath(script)}"'
        ])

    else:
        print(f"Unsupported platform: {system}")

'''
Runs the name server and the test script in separate terminals.
'''
if __name__ == "__main__":
    run_script_in_new_terminal("./replica.py")
    run_script_in_new_terminal("./server.py")
    run_script_in_new_terminal("./client.py")
