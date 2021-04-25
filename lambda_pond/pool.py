import multiprocessing
from multiprocessing.connection import Connection
import threading
import typing
from typing import Dict, Callable, Optional
import time
import queue

from lambda_pond.worker import Worker, DURATION_MICROSECOND
from lambda_pond.task import AsyncTask, TaskError

from queue import Queue


class PondTimeoutError(Exception):
    pass


class Pool:
    def __init__(self, size=multiprocessing.cpu_count(), task_timeout=None):
        self._task_timeout = task_timeout
        self._size = size
        self._workers: typing.List[Worker] = []
        self._results_queue = Queue()
        self._tasks_by_id: Dict[str, AsyncTask] = {}
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
        self._function_registry = {}

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wait()
        self.stop()

    def register(self, fn: Callable, name: Optional[str] = None):
        if name is None:
            name = f'{fn.__module__}.{fn.__name__}'
        self._function_registry[name] = fn
        return name

    def _worker_recycler(self):
        while not self._shutdown:
            stopped_workers_found = False
            for worker in self._workers:
                if worker.stopped:
                    stopped_workers_found = True
                    with worker.config.lock:
                        self._workers.remove(worker)
                        worker.stop()
                        worker.current_task.fulfil(
                            TaskError(
                                PondTimeoutError(f'timeout exceeded for worker {worker._id}')
                            )
                        )
                        self._tasks_by_id.pop(worker.current_task.identifier)
                        worker.set_current(None)

            closed_readers_found = False
            for reader, worker in list(self._reader_to_worker_map.items()):
                if worker.stopped and reader.closed:
                    closed_readers_found = True
                    with worker.config.lock:
                        del self._reader_to_worker_map[reader]
            if stopped_workers_found or closed_readers_found:
                self._start_workers()
            time.sleep(DURATION_MICROSECOND)

    def _dispatcher(self):
        while not self._shutdown:
            try:
                task = self._task_queue.get(block=True, timeout=DURATION_MICROSECOND)
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
        worker, result_reader = Worker.build(task_timeout=self._task_timeout, 
                                             lock=threading.Lock())
        self._reader_to_worker_map[result_reader] = worker
        worker.set_function_registry(self._function_registry)
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
        self._tasks_by_id.setdefault(task.identifier, task)
        self._task_queue.put_nowait(task)
        return task

    def _dispatch(self, task: AsyncTask):
        worker = self._assign_worker()
        with worker.config.lock:
            worker.send(task)

    def _assign_worker(self) -> Worker:
        return self._available_worker_queue.get(block=True)

    def _poll_for_updates(self):
        try:
            ready_connections = multiprocessing.connection.wait(
                self._reader_to_worker_map.keys(),
                timeout=DURATION_MICROSECOND
            )
        except OSError:
            return

        for result_reader in ready_connections:
            if not result_reader.closed:
                data = result_reader.recv()
            else:
                continue

            self._results_queue.put_nowait(data)
            self._completed_task_count += 1
            worker = self._reader_to_worker_map[result_reader]
            with worker.config.lock:
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
        print(f'waiting: {len(self._tasks_by_id)}')
        while self._tasks_by_id or \
                not self._task_queue.empty() or \
                not self._results_queue.empty():
            time.sleep(DURATION_MICROSECOND)
