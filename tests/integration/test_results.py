import unittest

from lambda_pond import Pool


def f(x):
    return x ** 2


class TestResults(unittest.TestCase):
    def test_all_tasks(self):
        tasks = []
        with Pool(size=2) as p:
            for n in range(20):
                tasks.append(p.apply_async(f, n))
        self.assertListEqual(
            list(map(f, range(20))),
            [task.result for task in tasks]
        )
