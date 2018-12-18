# -*- coding: utf8 -*-

import re
import requests
import sys
import inspect
from threading import Thread
from datetime import datetime, timedelta
import libs.common as CMN
g_logger = CMN.LOG.get_logger()


class WorkerThread(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
    	super(WorkerThread, self).__init__()
        self.tasks = tasks
        self.daemon = True
        self.web_scrapy_obj = None
        self.thread_index = None
        # self.ret_code = CMN.RET_SUCCESS
        self.errmsg = None
        self.errmsg_traceback = None
        self.task_done_success = False
        self.start()


    def __str__(self):
        return "Thread for %s" % self.web_scrapy_obj.get_description()


    def __record_full_stack_traceback(self):
       self.errmsg_traceback = CMN.FUNC.get_full_stack_traceback()


    @property
    def Instance(self):
        if self.web_scrapy_obj is None:
            raise ValueError
    	return self.web_scrapy_obj


    @property
    def Return(self):
    	return self.ret


    @property
    def ErrMsg(self):
        return self.errmsg


    @property
    def ErrMsgTrackback(self):
        return self.errmsg_traceback


    @property
    def IsTaskDone(self):
        return self.task_done_success


    @property
    def ThreadIndex(self):
        return self.thread_index


    def run(self):
        self.thread_index, self.web_scrapy_obj, func_name, args, kargs = self.tasks.get()
    	# import pdb; pdb.set_trace()
    	g_logger.debug("The thread for[%s::%s()] start...... %d" % (self.web_scrapy_obj.get_description(), func_name, self.thread_index))
        try:
            self.ret = getattr(self.web_scrapy_obj, func_name)(*args, **kargs)
            # print "Thread%d Return: %s, Error: %s" % (thread_index, self.thread_return, self.thread_errmsg)
            self.task_done_success = True
        except AttributeError:
            self.errmsg = "Class %s does not implement %s" % (self.web_scrapy_obj.__class__.__name__, func_name)
        except Exception, e:
            self.__record_full_stack_traceback()
            self.errmsg = "The thread for[%s] stop !!!, due to: %s" % (self.web_scrapy_obj.get_description(), str(e))
        finally:
            g_logger.debug("The thread for[%s::%s()] DONE...... %d" % (self.web_scrapy_obj.get_description(), func_name, self.thread_index))
            self.tasks.task_done()
