import multiprocessing

import pytest
from enhanced_queue import EnhancedQueue


@pytest.mark.parametrize("obj", ["A string", 0, 0.1, (0, 1), [0, 1], {"a": 1}])
def test_EnhancedQueue_singleprocess(obj):
    queue = EnhancedQueue(bufsize=1)

    for _ in range(10):
        print(f"Putting {obj!r}...")
        queue.put(obj)
        print(f"Getting {obj!r}...")
        new_obj = queue.get()

        assert obj == new_obj


def echo(input_queue: EnhancedQueue, output_queue: EnhancedQueue):
    while True:
        value = input_queue.get()
        if value is None:
            break

        output_queue.put(value)


@pytest.mark.parametrize("obj", ["A string", 0, 0.1, (0, 1), [0, 1], {"a": 1}])
def test_EnhancedQueue_p1c1(obj):
    input_queue = EnhancedQueue(bufsize=1)
    output_queue = EnhancedQueue(bufsize=1)

    child = multiprocessing.Process(target=echo, args=(input_queue, output_queue))
    child.start()

    for _ in range(10):
        print(f"Putting {obj!r}...")
        input_queue.put(obj)
        print(f"Getting {obj!r}...")
        new_obj = output_queue.get()

        assert obj == new_obj

    input_queue.put(None)


@pytest.mark.parametrize("obj", ["A string", 0, 0.1, (0, 1), [0, 1], {"a": 1}])
def test_EnhancedQueue_pncn(obj):
    input_queues = []
    output_queues = []
    workers = []
    for _ in range(4):
        input_queue = EnhancedQueue(bufsize=1)
        output_queue = EnhancedQueue(bufsize=1)

        input_queues.append(input_queue)
        output_queues.append(output_queue)

        worker = multiprocessing.Process(target=echo, args=(input_queue, output_queue))
        worker.start()
        workers.append(worker)

    for _ in range(10):
        print(f"Putting {obj!r}...")
        for iq in input_queues:
            iq.put(obj)

        print(f"Getting {obj!r}...")
        for oq in output_queues:
            new_obj = oq.get()
            assert obj == new_obj

    for iq in input_queues:
        iq.put(None)