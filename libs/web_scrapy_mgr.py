import threading


class WebSracpyMgr(Object):

	def __init__(self):
		self.max_concurrent_thread_amount = 4
		self.thread_pool_list = []

