import unittest

from lambda_pond import AsyncTask


class TestAsyncTask(unittest.TestCase):
    def test_ready(self):
        task = AsyncTask(fn=str)
        self.assertFalse(task.ready())
        task.fulfil(None)
        self.assertTrue(task.ready())

