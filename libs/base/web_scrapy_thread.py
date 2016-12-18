# -*- coding: utf8 -*-

import re
import requests
import sys
import inspect
from threading import Thread
from datetime import datetime, timedelta
import libs.common as CMN
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyThread(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
    	super(WebScrapyThread, self).__init__()
        self.tasks = tasks
        self.daemon = True
        self.start()
        self.web_scrapy_obj = None
        self.thread_index = None
        # self.ret_code = CMN.RET_SUCCESS
        self.errmsg = None
        self.errmsg_traceback = None
        self.task_done_success = False


    def __str__(self):
        return "Thread for %s" % self.web_scrapy_obj.get_description()


    def __record_full_stack_traceback(self):
       # tb = sys.exc_info()[2]
       # errmsg = ""
       # errmsg += 'Traceback (most recent call last):'
       # for item in reversed(inspect.getouterframes(tb.tb_frame)[1:]):
       #    errmsg += ' File "{1}", line {2}, in {3}\n'.format(*item)
       #    for line in item[4]:
       #       errmsg += ' ' + line.lstrip()
       #    for item in inspect.getinnerframes(tb):
       #       errmsg += ' File "{1}", line {2}, in {3}\n'.format(*item)
       #    for line in item[4]:
       #       errmsg += ' ' + line.lstrip()
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
            self.errmsg = "Class %s does not implement %s" % (my_cls.__class__.__name__, method_name)
        except Exception, e:
            self.__record_full_stack_traceback()
            self.errmsg = "The thread for[%s] stop !!!, due to: %s" % (self.web_scrapy_obj.get_description(), str(e))
        finally:
            self.tasks.task_done()
