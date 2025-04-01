from threading import Thread, Lock
from queue import Queue, Empty
import os
import json
import logging

class ThreadPool:
    """
    A thread pool to manage and execute jobs concurrently.
    """
    def __init__(self):
        """
        Initialize the thread pool with a specified number of threads.
        """
        self.num_threads = int(os.getenv("TP_NUM_OF_THREADS", os.cpu_count()))
        self.jobs = Queue()
        self.job_status = {}
        self.lock = Lock()
        self.shutting_down = False
        self.threads = []

        for _ in range(self.num_threads):
            t = TaskRunner(self.jobs, self.job_status, self.lock)
            self.threads.append(t)

        for t in self.threads:
            t.start()

    def submit_job(self, job_id, job_func):
        """
        Submit a job to the thread pool.
        """
        if self.shutting_down:
            raise Exception("Server is shutting down, no new jobs accepted.")

        with self.lock:
            self.job_status[job_id] = "running"
        self.jobs.put((job_id, job_func))

    def shutdown(self):
        """
        Shutdown the thread pool gracefully.
        """
        self.shutting_down = True
        for _ in self.threads:
            self.jobs.put(None)  # Signal threads to stop

    def has_pending_jobs(self):
        """
        Check if there are any pending jobs in the queue.
        """
        return any(status == "running" for status in self.job_status.values())

class TaskRunner(Thread):
    """"
    "A thread that processes jobs from the job queue."
    """
    def __init__(self, job_queue, job_status, lock):
        """
        Initialize the TaskRunner with a job queue and status dictionary.
        """
        super().__init__()
        self.job_queue = job_queue
        self.job_status = job_status
        self.lock = lock
        self.daemon = True

    def run(self):
        """"
        Run the thread to process jobs from the queue.
        """
        while True:
            try:
                item = self.job_queue.get(timeout=1)
                if item is None:
                    break
                job_id, job_func = item
                result = job_func()

                with open(f'results/{job_id}.json', 'w') as f:
                    json.dump(result, f)

                logging.info("Job %s completed and saved.", job_id)

                with self.lock:
                    self.job_status[job_id] = "done"

            except Empty:
                continue
