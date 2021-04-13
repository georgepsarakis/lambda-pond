# lambda-pond

A Process Pool for environments where semaphore support is unavailable ([`sem_open`](https://man7.org/linux/man-pages/man3/sem_open.3.html) and [`sem_init`](https://man7.org/linux/man-pages/man3/sem_init.3.html)), such as [AWS Lambda](https://aws.amazon.com/lambda/).

## Features

- Task Timeout
- Worker recycling after a certain amount of processed tasks
- TODO: Memory monitoring
- TODO: batch / `map` functionality

## Examples

```python
import hashlib

from lambda_pond import Pool

def f(x):
    return hashlib.sha1(('h' * (x + 1) * 10**8).encode()).hexdigest()        

with Pool(task_timeout=2) as p:
    tasks = []
    for n in range(10):
        t = p.apply_async(f, n)
        tasks.append(t)

for task in tasks:
    print(task.identifier, '->', task.result) 
```

## References

- https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/

## Credits

- API and multithreaded design using Queues inspired by the standard library [multiprocessing.Pool](https://github.com/python/cpython/blob/8fa1489365e7af8b90286c97db55a2dc60a05cde/Lib/multiprocessing/pool.py#L1).
- Worker pool concepts (timeout) inspired by [Celery](https://github.com/celery/celery).
