"""
Test: is gevent.queue.Queue.get(block=False) cooperative?

If cooperative, it yields to the gevent event loop on each call,
letting other greenlets run. If not, a tight loop starves them.
"""
import gevent
import gevent.queue


def test_get_block_false_cooperative():
    q = gevent.queue.Queue()
    side_ran = [0]

    def side_greenlet():
        # just increment a counter whenever we get CPU
        for _ in range(5):
            side_ran[0] += 1
            gevent.sleep(0)

    def consumer():
        # tight loop: no explicit yield, relies on get(block=False) to cooperate
        for _ in range(10_000):
            try:
                q.get(block=False)
            except gevent.queue.Empty:
                pass

    g_side = gevent.spawn(side_greenlet)
    g_consumer = gevent.spawn(consumer)

    gevent.joinall([g_consumer, g_side], timeout=5)

    print(f"side_greenlet ran {side_ran[0]} time(s) out of 5 expected")
    if side_ran[0] == 0:
        print("RESULT: get(block=False) is NOT cooperative — starved the side greenlet")
    else:
        print("RESULT: get(block=False) IS cooperative — side greenlet ran alongside the loop")

    return side_ran[0]


if __name__ == "__main__":
    test_get_block_false_cooperative()
