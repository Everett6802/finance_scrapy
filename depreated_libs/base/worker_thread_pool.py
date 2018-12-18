import re
from Queue import Queue
import scrapy_base
import worker_thread
from datetime import datetime
import libs.common as CMN
g_logger = CMN.LOG.get_logger()


class WorkerThreadPool(object):

    """Pool of threads consuming tasks from a queue"""
    SHOW_TRACEBACK = True
    def __init__(self, max_num_threads):
        self.wait_completion_done = False
        self.total_errmsg_str = None
        self.total_errmsg_traceback_str = None
        self.pool_size = max_num_threads # Max concurrent thread size
        # self.actual_thread_size = 0 # Actual concurrent thread size
        self.tasks = Queue(self.pool_size)
        self.worker_thread_list = []
        # self.thread_return_list = [None] * self.pool_size
        self.thread_error_list = [None] * self.pool_size
        self.worker_thread_count = 0
        for _ in range(max_num_threads): 
            worker = worker_thread.WorkerThread(self.tasks)
            self.worker_thread_list.append(worker)


    @property
    def PoolSize(self):
        return self.pool_size


    def __add_task(self, web_scrapy_obj, func_name, *args, **kargs):
        """Add a task to the queue"""
        if not isinstance(web_scrapy_obj, scrapy_base.ScrapyBase):
            raise ValueError("web_scrapy_obj should be derived from web_scrapy_base.ScrapyBase, not %s" % type(web_scrapy_obj))
        if self.worker_thread_count == self.pool_size:
            raise ValueError("The pool is full")
        g_logger.debug("Add a task; func_name: %s, args: %s, kargs: %s" % (func_name, args, kargs))
        self.tasks.put((self.worker_thread_count, web_scrapy_obj, func_name, args, kargs))
        self.worker_thread_count += 1
        # worker = WorkerThread(web_scrapy_obj, self.worker_thread_count)
        # self.worker_thread_list.append(worker)
        # self.tasks.put(worker)
        # self.worker_thread_count += 1
   

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()
        g_logger.debug("[%s] All threads are dead" % datetime.strftime(datetime.now(), '%H:%M:%S'))
# Update result
        errmsg = ""
        errmsg_traceback = ""
        for index, worker_thread in enumerate(self.worker_thread_list):
            if index == self.worker_thread_count:
                break
            if worker_thread.IsTaskDone:
                if worker_thread.ErrMsg is not None:
                    self.thread_error_list[index] = worker_thread.ErrMsg
                    errmsg += ("[%d] %s;" % (worker_thread.ThreadIndex, worker_thread.ErrMsg))
            else:
# Handle the issue when the thread abort for some reasons
                if worker_thread.ErrMsg is not None:
# Only get the first line from error message
                    errmsg += ("Worker thread[%d] Abort, due to %s;" % (worker_thread.ThreadIndex, worker_thread.ErrMsg))
                else:
                    errmsg += ("Worker thread[%d] Abort !!!;" % worker_thread.ThreadIndex)
# Record the trackback if exception occurs
                if worker_thread.ErrMsgTrackback is not None:
                    errmsg_traceback += ("### Worker thread[%d] ###\n%s\n" % (worker_thread.ThreadIndex, worker_thread.ErrMsgTrackback))

        if len(errmsg) > 0:
            self.total_errmsg_str = errmsg
        if len(errmsg_traceback) > 0:
            self.total_errmsg_traceback_str = errmsg_traceback

        self.wait_completion_done = True


    def get_error_list(self):
        if not self.wait_completion_done:
            raise ValueError("Not all threads are dead")
        return self.thread_error_list[0: self.worker_thread_count]


    @property
    def TotalErrorString(self):
        if not self.wait_completion_done:
            raise ValueError("Not all threads are dead")
        return self.total_errmsg_str


    @property
    def TotalErrorTracebackString(self):
        if not self.wait_completion_done:
            raise ValueError("Not all threads are dead")
        return self.total_errmsg_traceback_str


    def check_error_exist(self):
        return True if self.TotalErrorString is not None else False


    def add_scrape_web_to_csv_task(self, web_scrapy_obj):
        self.__add_task(web_scrapy_obj, "scrape_web_to_csv")


if __name__ == '__main__':
    from random import randrange
    from time import sleep

    delays = [randrange(1, 2) for i in range(5)]

    def wait_delay(d):
        print 'sleeping for (%d)sec' % d
        sleep(1)

    pool = ThreadPool(2)

    for i, d in enumerate(delays):
        pool.add_task(wait_delay, d)

    pool.wait_completion()
