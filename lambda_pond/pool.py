import multiprocessing
from multiprocessing.connection import Connection
import threading
import typing
from typing import Dict
import time
import queue

from lambda_pond.worker import Worker
from lambda_pond.task import AsyncTask, TaskError

from queue import Queue


class PondTimeoutError(Exception):
    pass


class Pool:
    def __init__(self, size=multiprocessing.cpu_count(), task_timeout=None):
        self._task_timeout = task_timeout
        self._size = size
        self._workers: typing.List[Worker] = []
        self._capacity = size
        self._results_queue = Queue()
        self._tasks_by_id: Dict[str, AsyncTask] = {}
        self._worker_state_lock = threading.Lock()
        self._available_worker_queue = Queue()
        self._shutdown = False
        self._results_collector_enabled = True
        self._dispatcher_enabled = True
        self._reader_to_worker_map: Dict[multiprocessing.Condition, Worker] = {}
        self._completed_task_count = 0
        self._task_queue = Queue()
        self._results_thread: typing.Union[None, threading.Thread] = None
        self._dispatcher_thread: typing.Union[None, threading.Thread] = None
        self._worker_recycler_thread: typing.Union[None, threading.Thread] = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wait()
        self.stop()

    def _worker_recycler(self):
        while not self._shutdown:
            for worker in self._workers:
                if worker.stopped:
                    with self._worker_state_lock:
                        self._workers.remove(worker)
                        worker.stop()
                        worker.current_task.fulfil(TaskError(PondTimeoutError(f'timeout exceeded for worker {worker._id}')))
                        self._tasks_by_id.pop(worker.current_task.identifier)

            closed_readers = []
            for reader, worker in self._reader_to_worker_map.items():
                if worker.stopped and reader.closed:
                    closed_readers.append(reader)
            with self._worker_state_lock:
                for reader in closed_readers:
                    del self._reader_to_worker_map[reader]
            self._start_workers()
            time.sleep(0.000001)

    def _dispatcher(self):
        while not self._shutdown:
            try:
                task = self._task_queue.get(block=True, timeout=0.001)
                self._dispatch(task)
            except queue.Empty:
                pass

    def _result_collector(self):
        while not self._shutdown:
            self._poll_for_updates()
            if self._results_queue.empty():
                continue
            task_id, result = self._results_queue.get()
            task = self._tasks_by_id.pop(task_id)
            task.fulfil(result)

    def _build_worker(self) -> Worker:
        worker, result_reader = Worker.build(task_timeout=self._task_timeout)
        worker.set_state_lock(self._worker_state_lock)
        self._reader_to_worker_map[result_reader] = worker
        return worker

    def _start_workers(self):
        current_size = len(self._workers)
        new_worker_count = self._size - current_size
        for worker_id in range(new_worker_count):
            worker = self._build_worker()
            self._workers.append(worker)
            pid = worker.start()
            print(f'Started worker with process ID: {pid}')
            self._available_worker_queue.put_nowait(worker)

    def start(self):
        self._results_thread = threading.Thread(target=self._result_collector, daemon=True)
        self._dispatcher_thread = threading.Thread(target=self._dispatcher, daemon=True)
        self._worker_recycler_thread = threading.Thread(target=self._worker_recycler, daemon=True)
        self._start_workers()
        self._results_thread.start()
        self._dispatcher_thread.start()
        self._worker_recycler_thread.start()
        return self

    def apply_async(self, function, *args, **kwargs) -> AsyncTask:
        task = AsyncTask(function, args=args, kwargs=kwargs)
        self._task_queue.put_nowait(task)
        return task

    def _dispatch(self, task: AsyncTask):
        worker = self._assign_worker()
        print(f'assigned worker {worker._id} for task: {task.identifier}')
        self._tasks_by_id.setdefault(task.identifier, task)
        with self._worker_state_lock:
            worker.set_current(task)
            worker.send(task.serialize())

    def _assign_worker(self) -> Worker:
        return self._available_worker_queue.get(block=True)

    def _poll_for_updates(self):
        wait_interval = 10**-6
        try:
            ready_connections = multiprocessing.connection.wait(
                self._reader_to_worker_map.keys(),
                timeout=wait_interval
            )
        except OSError:
            return

        for result_reader in ready_connections:
            with self._worker_state_lock:
                if not result_reader.closed:
                    data = result_reader.recv()
                else:
                    continue

            self._results_queue.put_nowait(data)
            self._completed_task_count += 1
            with self._worker_state_lock:
                worker = self._reader_to_worker_map[result_reader]
                self._available_worker_queue.put_nowait(worker)
                worker.set_current(None)

    def stop(self):
        print('stopping')
        self._shutdown = True
        for worker in self._workers:
            worker.stop()
        self._results_thread.join()
        self._worker_recycler_thread.join()
        self._dispatcher_thread.join()

    def wait(self):
        print('waiting...')
        print('pending tasks', len(self._tasks_by_id))
        while self._tasks_by_id or \
                not self._task_queue.empty() or \
                not self._results_queue.empty():
            time.sleep(0.00001)

