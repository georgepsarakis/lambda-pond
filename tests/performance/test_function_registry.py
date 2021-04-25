import time
import unittest

from lambda_pond import Pool


def f(n):
    return n ** 2


class TestRegisteredFunctions(unittest.TestCase):
    def test_function_registry(self):
        for _ in range(4):
            p = Pool(size=1)
            registered_function_name = p.register(f)
            p.start()
            tasks = []
            start = time.monotonic()
            for n in range(10 ** 3):
                tasks.append(p.apply_async(registered_function_name, n))
            p.wait()
            print(time.monotonic() - start)
            p.stop()
            self.assertEqual([task.result for task in tasks], list(map(f, range(10 ** 3))))

