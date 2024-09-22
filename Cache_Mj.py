# -*- coding: utf-8 -*-
"""Cache_Mj.ipynb

## KeyValueStore Server with Transactions and Concurrency
This is an in-memory key-value store that supports basic CRUD operations (PUT, GET, DELETE) and transaction management. The server supports multiple clients and ensures thread safety using locks. Each client or thread can start a transaction, commit, or rollback changes, all managed independently.

The server communicates over sockets using simple text commands and JSON responses.

### Features:
* Basic operations: PUT, GET, DEL
* Transaction support: START, COMMIT, ROLLBACK
* Thread-safe design to handle multiple clients concurrently
* Sockets for client-server communication

### Commands
Here is a list of supported commands:
* PUT [key] [value]: Adds or updates a key-value pair.
* GET [key]: Retrieves the value for a given key.
* DEL [key]: Deletes a key-value pair.
* START: Begins a transaction for the current thread/client.
* COMMIT: Commits changes made during a transaction.
* ROLLBACK: Discards changes made during a transaction.
* The server responds with JSON results, providing either the status or the result of the operation.

### Running the Server
Uncomment the server startup lines in the if __name__ == "__main__": block
```python
if __name__ == "__main__":
    HOST = '127.0.0.1'  # Localhost
    PORT = 65432  # Arbitrary non-privileged port
    start_server(HOST, PORT)
```

### Running the Client
You can use a simple socket client or a tool like `telnet` or `nc` to send commands to the server.
`nc 127.0.0.1 65432`

Then, you can start sending commands like:
```bash
PUT mykey myvalue
GET mykey
START
PUT tempkey tempvalue
COMMIT
```
"""

import socket
import json
import threading

class KeyValueStore:
    """
    In-memory key-value store with transaction support and thread-safe access.
    Supports basic operations such as PUT, GET, DELETE, and transaction handling.
    Each thread has its own transaction, and a lock ensures thread safety for global store access.
    """

    def __init__(self):
        """
        Initializes the key-value store.
        - self.store: The main store where committed key-value pairs are kept.
        - self.transactions: Dictionary holding per-thread active transactions.
        - self.lock: A threading lock to ensure thread-safe access to the store.
        """
        self.store = {}  # Main store for key-value pairs.
        self.transactions = {}  # Per-thread transactions.
        self.lock = threading.Lock()  # Lock to ensure thread-safe access to the store.

    def put(self, key, value):
        """
        Puts or updates a key-value pair in the store.
        If a transaction is active for the current thread, the change is stored in the transaction.
        Otherwise, it is committed directly to the main store.

        Args:
            key: The key to be added or updated.
            value: The value associated with the key.

        Returns:
            dict: Response indicating the status of the operation.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id in self.transactions:
                self.transactions[thread_id][key] = value
            else:
                self.store[key] = value
            return {"status": "Ok"}

    def get(self, key):
        """
        Retrieves the value of a key from the store.
        If a transaction is active for the current thread, it checks the transaction store first.
        Otherwise, it retrieves the value from the main store.

        Args:
            key: The key to retrieve the value for.

        Returns:
            dict: Response with the value if found, or an error message.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id in self.transactions and key in self.transactions[thread_id]:
                return {"status": "Ok", "result": self.transactions[thread_id][key]}
            elif key in self.store:
                return {"status": "Ok", "result": self.store[key]}
            else:
                return {"status": "Error", "mesg": f"Key '{key}' not found."}

    def delete(self, key):
        """
        Deletes a key-value pair from the store.
        If a transaction is active for the current thread, it marks the key for deletion in the transaction.
        Otherwise, it directly deletes the key from the main store.

        Args:
            key: The key to be deleted.

        Returns:
            dict: Response indicating the status of the operation.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id in self.transactions:
                self.transactions[thread_id][key] = None
            else:
                if key in self.store:
                    del self.store[key]
            return {"status": "Ok"}

    def start_transaction(self):
        """
        Starts a new transaction for the current thread.
        If a transaction is already active for the thread, it returns an error.

        Returns:
            dict: Response indicating the status of the operation.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id in self.transactions:
                return {"status": "Error", "mesg": "Transaction already active."}
            self.transactions[thread_id] = {}
            return {"status": "Ok"}

    def commit_transaction(self):
        """
        Commits the current transaction for the thread, applying all changes to the main store.

        Returns:
            dict: Response indicating the status of the operation.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id not in self.transactions:
                return {"status": "Error", "mesg": "No active transaction."}

            for key, value in self.transactions[thread_id].items():
                if value is None:
                    if key in self.store:
                        del self.store[key]
                else:
                    self.store[key] = value

            del self.transactions[thread_id]
            return {"status": "Ok"}

    def rollback_transaction(self):
        """
        Rolls back the current transaction for the thread, discarding all uncommitted changes.

        Returns:
            dict: Response indicating the status of the operation.
        """
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id not in self.transactions:
                return {"status": "Error", "mesg": "No active transaction."}
            del self.transactions[thread_id]
            return {"status": "Ok"}

# Function to simulate a thread performing store operations
def simulate_store_operations(store, thread_id):
    """
    A function to simulate store operations in a thread.
    Each thread performs a sequence of PUT, GET, and DELETE operations.

    Args:
        store: The shared KeyValueStore instance.
        thread_id: The identifier for the thread.
    """
    key = f"key_{thread_id}"  # Each thread will work with its own key.

    # Start a transaction
    print(f"Thread {thread_id}: Start transaction -> {store.start_transaction()}")

    # Simulate PUT operation inside a transaction.
    store.put(key, f"value_{thread_id}")
    print(f"Thread {thread_id}: Put {key} -> value_{thread_id}")

    # Simulate GET operation inside a transaction.
    result = store.get(key)
    print(f"Thread {thread_id}: Get {key} -> {result}")

    # Commit the transaction
    print(f"Thread {thread_id}: Commit transaction -> {store.commit_transaction()}")

    # Simulate DELETE operation outside a transaction.
    store.delete(key)
    print(f"Thread {thread_id}: Deleted {key}")

def handle_client(client_socket, store):
    """
    Function to handle a single client connection. It listens for commands from the client,
    executes the corresponding operation on the key-value store, and sends back the result.

    Args:
        client_socket: The socket object representing the client connection.
        store: The shared KeyValueStore instance.
    """
    with client_socket:
        while True:
            try:
                # Receive data from the client
                data = client_socket.recv(1024).decode('utf-8').strip()
                if not data:
                    break

                # Parse the received command and arguments
                command_parts = data.split()
                if not command_parts:
                    continue

                command = command_parts[0].upper()

                # Handle commands
                if command == 'PUT':
                    key = command_parts[1]
                    value = ' '.join(command_parts[2:])
                    response = store.put(key, value)

                elif command == 'GET':
                    key = command_parts[1]
                    response = store.get(key)

                elif command == 'DEL':
                    key = command_parts[1]
                    response = store.delete(key)

                elif command == 'START':
                    response = store.start_transaction()

                elif command == 'COMMIT':
                    response = store.commit_transaction()

                elif command == 'ROLLBACK':
                    response = store.rollback_transaction()

                else:
                    response = {"status": "Error", "mesg": "Unknown command."}

                # Send the response back to the client as JSON
                client_socket.sendall(json.dumps(response).encode('utf-8'))

            except Exception as e:
                # Handle exceptions and notify the client
                error_message = {"status": "Error", "mesg": str(e)}
                client_socket.sendall(json.dumps(error_message).encode('utf-8'))

def start_server(host, port):
    """
    Starts the key-value store server that listens for incoming client connections.
    Each client connection is handled in a separate thread.

    Args:
        host: The hostname or IP address to bind the server to.
        port: The port number to bind the server to.
    """
    store = KeyValueStore()  # Shared key-value store across all clients.

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Server started on {host}:{port}")

    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")

        # Handle each client connection in a new thread
        client_thread = threading.Thread(target=handle_client, args=(client_socket, store))
        client_thread.start()




if __name__ == "__main__":
    # Uncomment lines below to open a socket channel
    '''
    HOST = '127.0.0.1'  # Localhost
    PORT = 65432  # Arbitrary non-privileged port
    start_server(HOST, PORT)
    '''

    # Create an instance of the KeyValueStore.
    store = KeyValueStore()

    # Create multiple threads to simulate concurrent access to the store.
    threads = []

    for i in range(5):
        # Create a thread that simulates store operations.
        t = threading.Thread(target=simulate_store_operations, args=(store, i))
        threads.append(t)
        t.start()  # Start the thread.

    # Wait for all threads to complete.
    for t in threads:
        t.join()

    # Print the final state of the store.
    print("Final store state:", store.store)

