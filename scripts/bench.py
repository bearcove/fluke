#!/usr/bin/env -S PYTHONUNBUFFERED=1 FORCE_COLOR=1 uv run

import ctypes
import os
import signal
import subprocess
import sys
import time
import openpyxl
from openpyxl.styles import Alignment

from pathlib import Path
from termcolor import colored
import pandas as pd
import tempfile

# Change to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

PERF_EVENTS = [
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "page-faults",
    "syscalls:sys_enter_write",
    "syscalls:sys_enter_writev",
    "syscalls:sys_enter_epoll_wait",
    "syscalls:sys_enter_io_uring_enter"
]
PERF_EVENTS_STRING = ",".join(PERF_EVENTS)

# Define constants for prctl
PR_SET_PDEATHSIG = 1

# Load the libc shared library
libc = ctypes.CDLL("libc.so.6")

def set_pdeathsig():
    """Set the parent death signal to SIGKILL."""
    # Call prctl with PR_SET_PDEATHSIG and SIGKILL
    libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)

# Set trap to kill the process group on script exit
def kill_group():
    os.killpg(0, signal.SIGKILL)

def abort_and_cleanup(signum, frame):
    print(colored("\nAborting and cleaning up...", "red"))
    kill_group()
    sys.exit(1)

# Register signal handlers
signal.signal(signal.SIGINT, abort_and_cleanup)
signal.signal(signal.SIGTERM, abort_and_cleanup)

# Create directory if it doesn't exist
Path("/tmp/loona-perfstat").mkdir(parents=True, exist_ok=True)

# Kill older processes
for pidfile in Path("/tmp/loona-perfstat").glob("*.PID"):
    if pidfile.is_file():
        with open(pidfile) as f:
            pid = f.read().strip()
        if pid != str(os.getpid()):
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        pidfile.unlink()

# Kill older remote processes
subprocess.run(["ssh", "brat", "pkill -9 h2load"], check=False)

LOONA_DIR = os.path.expanduser("~/bearcove/loona")

# Build the servers
subprocess.run(["cargo", "build", "--release", "--manifest-path", f"{LOONA_DIR}/Cargo.toml", "-F", "tracing/release_max_level_info"], check=True)

# Set protocol, default to h2c
PROTO = os.environ.get("PROTO", "h2c")
os.environ["PROTO"] = PROTO

# Get the default route interface
default_route = subprocess.check_output(["ip", "route", "show", "0.0.0.0/0"]).decode().strip()
default_interface = default_route.split()[4]

# Get the IP address of the default interface
ip_addr_output = subprocess.check_output(["ip", "addr", "show", "dev", default_interface]).decode().strip()

OUR_PUBLIC_IP = None
for line in ip_addr_output.split('\n'):
    if 'inet ' in line:
        OUR_PUBLIC_IP = line.split()[1].split('/')[0]
        break

if not OUR_PUBLIC_IP:
    print(colored("Error: Could not determine our public IP address", "red"))
    sys.exit(1)

print(f"📡 Our public IP address is {OUR_PUBLIC_IP}")

# Declare h2load args based on PROTO
HTTP_OR_HTTPS = "https" if PROTO == "tls" else "http"

# Launch hyper server
hyper_env = os.environ.copy()
hyper_env.update({"ADDR": "0.0.0.0", "PORT": "8001"})
hyper_process = subprocess.Popen([f"{LOONA_DIR}/target/release/httpwg-hyper"], env=hyper_env, preexec_fn=set_pdeathsig)
HYPER_PID = hyper_process.pid
with open("/tmp/loona-perfstat/hyper.PID", "w") as f:
    f.write(str(HYPER_PID))

# Launch loona server
loona_env = os.environ.copy()
loona_env.update({"ADDR": "0.0.0.0", "PORT": "8002"})
loona_process = subprocess.Popen([f"{LOONA_DIR}/target/release/httpwg-loona"], env=loona_env, preexec_fn=set_pdeathsig)
LOONA_PID = loona_process.pid
with open("/tmp/loona-perfstat/loona.PID", "w") as f:
    f.write(str(LOONA_PID))

HYPER_ADDR = f"{HTTP_OR_HTTPS}://{OUR_PUBLIC_IP}:8001"
LOONA_ADDR = f"{HTTP_OR_HTTPS}://{OUR_PUBLIC_IP}:8002"

servers = {
    "hyper": (HYPER_PID, HYPER_ADDR),
    "loona": (LOONA_PID, LOONA_ADDR)
}

SERVER = os.environ.get("SERVER")
if SERVER:
    if SERVER in servers:
        servers = {SERVER: servers[SERVER]}
    else:
        print(f"Error: SERVER '{SERVER}' not found in the list of servers.")
        sys.exit(1)

H2LOAD = "/nix/var/nix/profiles/default/bin/h2load"

endpoint = os.environ.get("ENDPOINT", "/repeat-4k-blocks/128")
rps = os.environ.get("RPS", "2")
clients = os.environ.get("CLIENTS", "20")
streams = os.environ.get("STREAMS", "2")
warmup = int(os.environ.get("WARMUP", "5"))
duration = int(os.environ.get("DURATION", "20"))
repeat = int(os.environ.get("REPEAT", "3"))

# Mode can be 'perfstat' or 'samply'
mode = os.environ.get("MODE", "perfstat")

loona_git_sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=os.path.expanduser('~/bearcove/loona')).decode().strip()

benchmark_params = f"RPS={rps}, CLIENTS={clients}, STREAMS={streams}, WARMUP={warmup}, DURATION={duration}, REPEAT={repeat}"
print(colored(f"📊 Benchmark parameters: {benchmark_params}", "blue"))

def gen_h2load_cmd(addr: str):
    return [
        H2LOAD,
        "--rps", rps,
        "--clients", clients,
        "--max-concurrent-streams", streams,
        "--duration", str(duration),
        f"{addr}{endpoint}",
    ]

def do_samply():
    if len(servers) > 1:
        print(colored("Error: More than one server specified.", "red"))
        print("Please use SERVER=[loona,hyper] to narrow down to a single server.")
        sys.exit(1)

    for server, (PID, ADDR) in servers.items():
        print(colored("Warning: Warmup period is not taken into account in samply mode yet.", "yellow"))

        samply_process = subprocess.Popen(["samply", "record", "-p", str(PID)], preexec_fn=set_pdeathsig)
        with open("/tmp/loona-perfstat/samply.PID", "w") as f:
            f.write(str(samply_process.pid))
            h2load_cmd = gen_h2load_cmd(ADDR)
            subprocess.run(["ssh", "brat"] + h2load_cmd, check=True)
        samply_process.send_signal(signal.SIGINT)
        samply_process.wait()

def do_perfstat():
    all_data = {}

    pickle_filename = f"/tmp/loona-perfstat/combined_performance.pkl"
    combined_df = None

    if os.environ.get("RELOAD_DF") == "1":
        if os.path.exists(pickle_filename):
            combined_df = pd.read_pickle(pickle_filename)
            print(colored(f"Loaded DataFrame from {pickle_filename}", "green"))
            return
        else:
            print(colored(f"Error: File {pickle_filename} does not exist.", "red"))
            sys.exit(1)

    if combined_df is None:
        for server, (PID, ADDR) in servers.items():
            print(colored(f"🚀 Benchmarking {open(f'/proc/{PID}/cmdline').read().replace(chr(0), ' ').strip()}", "yellow"))

            print(colored(f"🏃 Measuring {server} 🏃 (will take {duration} seconds)", "magenta"))
            output_path = tempfile.NamedTemporaryFile(delete=False, suffix='.csv').name

            perf_cmd = [
                "perf", "stat",
                "--event", PERF_EVENTS_STRING,
                "--field-separator", ",",
                "--output", output_path,
                "--pid", str(PID),
                "--delay", str(warmup*1000),
                "--repeat", str(repeat),
                "--",
                "ssh", "brat",
            ] + gen_h2load_cmd(ADDR)
            if PROTO == "tls":
                perf_cmd += ["--alpn-list", "h2"]

            perf_process = subprocess.Popen(perf_cmd, preexec_fn=set_pdeathsig)
            perf_process.wait()

            # Read the file, skipping lines starting with '#' and empty lines
            csv_data = ""
            with open(output_path, 'r') as f:
                for line in f:
                    if not line.startswith('#') and line.strip():
                        csv_data += line


            # Print the contents of output_path
            print(f"Raw CSV data:")
            print(csv_data)

            # Define the correct columns
            column_names = ['value', 'unit', 'event', 'stddev']
            perf_output = pd.read_csv(output_path, usecols=range(4), names=column_names)
            os.unlink(output_path)

            print("Performance Output (initial):")
            print(perf_output)

            # Clean up the data
            perf_output['event'] = perf_output['event'].str.strip()
            perf_output['value'] = pd.to_numeric(perf_output['value'], errors='coerce')

            # Format values as billions for specific events
            for index, row in perf_output.iterrows():
                event = row['event']
                if isinstance(event, str) and event != 'page-faults' and not event.startswith('syscalls:'):
                    perf_output.loc[index, 'value'] /= 1e9
                    perf_output.loc[index, 'unit'] = 'B'

            # Round values to 3 decimal places
            perf_output['value'] = perf_output['value'].round(3)

            # Strip 'syscalls:sys_enter_' prefix from event names
            perf_output['event'] = perf_output['event'].str.replace('syscalls:sys_enter_', '', regex=False)

            print("Performance Output (cleaned):")
            print(perf_output)

            # Create a DataFrame directly from the cleaned data
            df = perf_output.set_index('event')[['value', 'unit', 'stddev']]

            all_data[server] = df

        # Combine data from all servers
        combined_df = pd.concat(all_data, axis=1)

        # Reorder columns to group value, unit, stddev for each server
        new_columns = []
        for server in servers.keys():
            new_columns.extend([(server, 'value'), (server, 'unit'), (server, 'stddev')])
        combined_df = combined_df.reindex(columns=pd.MultiIndex.from_tuples(new_columns))

        # Save the DataFrame to disk in a pickle format
        combined_df.to_pickle(pickle_filename)
        print(colored(f"Saved DataFrame to {pickle_filename}", "green"))

    print("Combined DataFrame:")
    print(combined_df)

    # Generate sheet name with current date, loona git hash, and benchmark params
    current_date = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
    sheet_name = f"{current_date}_{loona_git_sha}_{benchmark_params.replace(', ', '_')}"

    print(f"Sheet name: {sheet_name}")

    # Save the combined DataFrame as an Excel file
    excel_filename = f"/tmp/loona-perfstat/combined_performance.xlsx"
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]

    print(colored(f"Saved combined performance data to {excel_filename}", "green"))

try:
    if mode == 'perfstat':
        do_perfstat()
    elif mode == 'samply':
        do_samply()
    else:
        print(colored(f"Error: Unknown mode '{mode}'", "red"))
        print(colored("Known modes:", "yellow"))
        print(colored("- perfstat", "yellow"))
        print(colored("- samply", "yellow"))
        sys.exit(1)
except KeyboardInterrupt:
    print(colored("\nKeyboard interrupt detected. Cleaning up...", "red"))
    abort_and_cleanup(None, None)

kill_group()