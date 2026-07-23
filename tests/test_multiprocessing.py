import multiprocessing
import unittest

from yfinance_cache import yfc_dat
from yfinance_cache import yfc_multi


def _check_worker_locks(_):
    """Top-level so that spawn and forkserver can pickle it."""
    lock = yfc_dat.get_exchange_lock("NYQ")
    with lock:
        return yfc_multi._progress_queue is not None


class TestMultiprocessingLocks(unittest.TestCase):
    def test_normal_exchange_lock_does_not_start_a_process(self):
        before = {p.pid for p in multiprocessing.active_children()}
        lock = yfc_dat.get_exchange_lock("NYQ")
        with lock:
            pass
        after = {p.pid for p in multiprocessing.active_children()}
        self.assertEqual(after, before)

    def test_pool_locks_work_with_all_available_contexts(self):
        for method in multiprocessing.get_all_start_methods():
            with self.subTest(method=method):
                ctx = multiprocessing.get_context(method)
                locks = {
                    exchange: ctx.Lock()
                    for exchange in yfc_dat.exchangeToXcalExchange
                }
                queue = ctx.Queue()
                try:
                    with ctx.Pool(
                        2,
                        initializer=yfc_multi.reinitialize_locks,
                        initargs=(locks, queue),
                    ) as pool:
                        result = pool.map(_check_worker_locks, range(4))
                    self.assertEqual(result, [True] * 4)
                finally:
                    queue.close()
                    queue.join_thread()


if __name__ == "__main__":
    unittest.main()
