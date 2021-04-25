import uuid
import traceback
from dataclasses import dataclass
from typing import Callable, Tuple, Dict


class TaskError(Exception):
    def __init__(self, exc: Exception):
        self._original_exception = exc
        self._tb = None

    @property
    def tb(self):
        if self._tb is None:
            self._tb = traceback.format_tb(self._original_exception.__traceback__)
        return self._tb

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._original_exception)})'


@dataclass
class TaskWrapper:
    function: Callable
    args: Tuple
    kwargs: Dict
    identifier: str

    def execute(self, registry=None):
        if registry is not None and isinstance(self.function, str):
            function = registry[self.function]
        else:
            function = self.function
        if function is None:
            return None
        return function(*self.args, **self.kwargs)


class AsyncTask:
    _NO_RESULT = object()

    def __init__(self, fn, args=None, kwargs=None, identifier=None):
        self._fn = fn
        self._args = args or ()
        self._kwargs = kwargs or {}
        self._result = self._NO_RESULT
        self._id = identifier or str(uuid.uuid4())

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def function(self):
        return self._fn

    @property
    def failed(self):
        return isinstance(self._result, TaskError)

    @property
    def result(self):
        return self._result

    def ready(self) -> bool:
        return self._result is not self._NO_RESULT

    def successful(self):
        return not self.failed and self.ready()

    def wait(self):
        pass

    def serialize(self):
        return self.identifier, self._fn, self._args, self._kwargs

    @classmethod
    def deserialize(cls, serialized) -> 'AsyncTask':
        identifier, fn, args, kwargs = serialized
        return cls(
            fn=fn,
            args=args,
            kwargs=kwargs,
            identifier=identifier
        )

    @property
    def identifier(self):
        return self._id

    def fulfil(self, result):
        self._result = result

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} fn={self._fn},args={self._args},' \
               f'kwargs={self._kwargs},ready={self.ready()}>'

    def __str__(self) -> str:
        return repr(self)
