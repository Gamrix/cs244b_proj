"""
Profiling Tools

Some Utility functions to help time and profile the performance of PyRTL code

Copied from other places I have been using it
"""
from contextlib import contextmanager
import time


class ProgTimer(object):
    def __init__(self):
        self.start_timing()

    def start_timing(self):
        try:
            self.start_time_perf = time.perf_counter()
            self.start_time_proc = time.process_time()
            self.precise = True
        except Exception:
            self.start_time = time.clock()
            self.precise = False

    def finish_timing(self):
        """End timing and return the time that has passed"""
        if self.precise:
            exec_time = time.perf_counter() - self.start_time_perf
            if exec_time > 60:
                # remove the sleep time from long running sections
                exec_time = time.process_time() - self.start_time_proc
        else:
            exec_time = time.clock() - self.start_time
        return exec_time


@contextmanager
def time_func(name=""):
    timer = ProgTimer()
    try:
        yield
    finally:
        exec_time = timer.finish_timing()
        if timer.precise:
            print("{} execution time: {}".format(name, exec_time))
        else:
            print("{} wall-clock time: {}".format(name, exec_time))


@contextmanager
def profile(name=""):
    import cProfile
    import pstats

    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()

    print("Profile for " + name)
    sortby = 'cumulative'
    ps = pstats.Stats(pr).sort_stats(sortby)
    ps.print_stats()
