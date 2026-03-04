import signal, time

def handler(signum, frame):
    print("Caught", signum, flush=True)

signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)
print("Waiting for signals...", flush=True)
while True:
    time.sleep(1)
