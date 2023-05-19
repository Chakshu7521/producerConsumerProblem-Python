import multiprocessing 
from multiprocessing import Process, Lock
from multiprocessing import Manager
import time, os

# Producer function
def produce(event, lock, buffer):
    with open("keys.txt", "r") as file:
        keys = file.readlines()
    
    # Produce items using the read keys
    for i in range(0, len(keys), 10):
        # Get 10 keys at a time
        batch_keys = keys[i:i+10]
        
        for key in batch_keys:
            key = key.strip()
            item = {}
            # Produce items related to the key
            
            item["key"] = key
            item["processId"] = os.getpid() 
            item["time"] = time.time()
            lock.acquire()  # Acquire the lock before accessing the buffer
            index = len(buffer)
            item["index"] = index
            buffer.append(item)
            print(f"Produced: {item}")
            lock.release()  # Release the lock
            time.sleep(0.5)  # Simulate time for updating the buffer

# Consumer function
def consume(lock, buffer):
    while True:
        lock.acquire()  # Acquire the lock before accessing the buffer
        if len(buffer) == 0:
            lock.release()  # Release the lock if the buffer is empty
            time.sleep(0.5)
        else:
            index = len(buffer)-1
            item = buffer.pop()
            t = time.time()
            print(f"Consumed: {item}, from index {index},time {t}")
            lock.release()  # Release the lock
            time.sleep(0.5)  # Simulate time for updating the buffer

def generate_keys(num_keys):
    with open("keys.txt", "w") as file:
        for i in range(1, num_keys + 1):
            key = str(i)
            file.write(key + "\n")

if __name__ == "__main__":
    # Create a lock object
    lock = multiprocessing.Lock()
    manager = multiprocessing.Manager()
    # Create a shared list for the buffer
    buffer = manager.list()

    # Create the event
    event = multiprocessing.Event()

    # Create the producer process
    producer_process = multiprocessing.Process(target=produce, args=(event, lock, buffer))

    # Create the consumer process
    consumer_process = multiprocessing.Process(target=consume, args=(lock, buffer))

    # Generate keys and write to the file
    generate_keys(100)

    

    # Start the producer process
    producer_process.start()
 
    # Start the consumer process
    consumer_process.start()

    # Wait for the producer process to finish
    producer_process.join()

    # Wait for the consumer process to finish
    consumer_process.join()

    print("Done")
