import time
import unittest

from lambda_pond import Pool

def f():
    pass


class TestBenchmark(unittest.TestCase):
    def test_baseline(self) -> None:
        rounds = 10 ** 3
        # serial execution
        start = time.monotonic()
        for _ in range(rounds):
            f()
        serial_execution_duration = time.monotonic() - start

        parallel_execution_duration = {}
        for size in range(1, 5):
            start = time.monotonic()
            with Pool(size=size) as p:
                for _ in range(rounds):
                    p.apply_async(f)
            parallel_execution_duration[size] = time.monotonic() - start
        print(serial_execution_duration)
        print(parallel_execution_duration)