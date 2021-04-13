from hashlib import sha1
import threading
import time

from lambda_pond import Pool


def f(x):
    result = ''
    s = ''
    for m in range(10**3):
        s += str(m) + str(x)
        result = sha1(s.encode()).hexdigest()
    return result


def lambda_handler(event, context):
    start = time.time()
    with Pool(task_timeout=1) as p:
        tasks = []
        for n in range(10):
            t = p.apply_async(f, n)
            tasks.append(t)

    print(">>>", len(tasks))
    for t in tasks:
        print(t.result)

    print(">>> multiprocessing: ", time.time() - start)

    print('Active threads:', threading.enumerate())

    start = time.time()

    print(">>>", len(tasks))
    for t in tasks:
        print(t.result)

    for n in range(10):
        print(n)
        f(n)

    print("--- sequential", time.time() - start)


if __name__ == "__main__":
    lambda_handler(None, None)
    print(threading.enumerate())
