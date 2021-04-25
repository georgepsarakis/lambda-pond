import multiprocessing
from multiprocessing.connection import Connection
import uuid
from dataclasses import dataclass
from typing import Callable, Tuple
import time
from lambda_pond.task import TaskError, AsyncTask
import threading


@dataclass
class Configuration:
    result_reader: Connection
    result_writer: Connection
    task_reader: Connection
    task_writer: Connection
    lock: threading.Lock
    process_name: str = None
    on_after_fork: Callable = None
    on_shutdown: Callable = None
    max_tasks: int = None
    task_timeout: int = None


@dataclass
class Statistics:
    total_task_count: int
    failed_task_count: int


class Timer:
    def __init__(self):
        self._start = time.monotonic()

    def reset(self):
        self._start = time.monotonic()

    def elapsed(self):
        return time.monotonic() - self._start


DURATION_MICROSECOND = 10 ** -6
DURATION_MILLISECOND = 10 ** -3


_SHUTDOWN_OBJECT = object()


def _shutdown(*args, **kwargs):
    return _SHUTDOWN_OBJECT


class Executor:
    def __init__(self, function_registry):
        self._function_registry = function_registry

    def run(self, task: AsyncTask):
        fn = task.function
        callable_fn = self._function_registry.get(fn, fn)
        return callable_fn(*task.args, **task.kwargs)


class Worker:
    def __init__(self, config: Configuration):
        self._config = config
        self._task_reader = self.config.task_reader
        self._task_writer = self.config.task_writer
        self._result_writer = self.config.result_writer
        self._result_reader = self.config.result_reader

        self._id = str(uuid.uuid4())
        self._name = self.config.process_name or f'lambda-wp-{self._id}'
        self._process: multiprocessing.Process = None
        self._pid = None
        self._stats = Statistics(failed_task_count=0, total_task_count=0)
        self._current_task = None
        self._timer = Timer()
        self._timeout_watcher_thread: threading.Thread = None
        self._timeout_exceeded = False
        self._stopped = False
        self._function_registry = {}
        self._executor = Executor(self._function_registry)

    def set_function_registry(self, mapping: dict):
        self._function_registry.update(mapping)

    @property
    def timer(self) -> Timer:
        return self._timer

    @property
    def current_task(self) -> AsyncTask:
        return self._current_task

    @property
    def timeout(self):
        return self._config.task_timeout

    def set_current(self, task):
        self._current_task = task
        self._timer.reset()

    @classmethod
    def build(cls, name=None, **kwargs) -> Tuple['Worker', multiprocessing.connection.Connection]:
        task_reader, task_writer = multiprocessing.Pipe()
        result_reader, result_writer = multiprocessing.Pipe()
        config = Configuration(
            task_reader=task_reader,
            task_writer=task_writer,
            result_reader=result_reader,
            result_writer=result_writer,
            process_name=name,
            **kwargs
        )
        return cls(config), result_reader

    @property
    def alive(self):
        print(self._id, self._timeout_exceeded)
        return not self._stopped and not self._timeout_exceeded and self.process.is_alive()

    @property
    def config(self) -> Configuration:
        return self._config

    def start(self) -> int:
        self._process = multiprocessing.Process(
            name=self._name,
            target=self._loop,
            kwargs={
                'task_reader': self._task_reader,
                'result_writer': self._result_writer,
            },
            daemon=True
        )
        self._process.start()
        self._pid = self._process.pid
        if self._config.task_timeout is not None:
            self._timeout_watcher_thread = threading.Thread(target=self._monitor_task_timeout)
            self._timeout_watcher_thread.daemon = True
            self._timeout_watcher_thread.start()
        return self._pid

    def _monitor_task_timeout(self):
        while not self.stopped:
            time.sleep(DURATION_MILLISECOND)
            if self._timer.elapsed() > self._config.task_timeout:
                with self._config.lock:
                    print('timeout exceeded!')
                    self._timeout_exceeded = True
                    self._process.terminate()
                    self._timer.reset()
                    self.stop(force=True)

    @property
    def process(self) -> multiprocessing.Process:
        return self._process

    def _loop(self, task_reader: Connection, result_writer: Connection):
        if self.config.on_after_fork is not None:
            self.config.on_after_fork()

        executor = self._executor.run
        task_deserializer = AsyncTask.deserialize
        while True:
            if task_reader.poll(DURATION_MICROSECOND):
                message = task_reader.recv()
                task = task_deserializer(message)
                try:
                    result = executor(task)
                    if result is _SHUTDOWN_OBJECT:
                        break
                except Exception as e:
                    result = TaskError(e)
                result_writer.send((task.identifier, result))

        if self.config.on_shutdown is not None:
            self.config.on_shutdown()

    def send(self, task: AsyncTask) -> bool:
        self.set_current(task)
        serialized_task = task.serialize()
        try:
            self._task_writer.send(serialized_task)
        except ConnectionError:
            self.stop(force=True, graceful=False)
        except OSError:
            return False
        return True

    def receive(self):
        return self._result_reader.recv()

    @property
    def stopped(self):
        return self._stopped

    def stop(self, force=False, graceful=True):
        self._stopped = True
        if graceful:
            self.send(AsyncTask(fn=_shutdown))
        total = 0
        while self.process.is_alive():
            time.sleep(DURATION_MICROSECOND)
            total += DURATION_MICROSECOND
            if force and total > 0.1:
                self._process.terminate()
                if total > 1:
                    self._process.kill()
        print(f'closing {self._pid}')
        try:
            self.config.task_reader.close()
            self.config.task_writer.close()
            self.config.result_reader.close()
            self.config.result_writer.close()
        # TODO: check errors
        except (ConnectionError, OSError):
            pass
        return self.process.terminate()
