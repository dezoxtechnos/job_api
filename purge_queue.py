# purge_queue.py
from kombu import Connection, Queue
BROKER = "redis://localhost:6379/0"
QUEUE_NAME = "score"
with Connection(BROKER) as conn:
    q = Queue(QUEUE_NAME, channel=conn)
    bound = q(conn)
    n = bound.purge()
    print(f"Purged {n} messages from queue '{QUEUE_NAME}'")
