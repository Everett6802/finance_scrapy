# -*- coding: utf8 -*-

import os
import logging
import time
import tarfile
import inspect
from threading import Thread
from threading import Lock


class LogRotate(Thread):

    LOG_WORKING_FOLDER = "~"
    LOG_FILE_FOLDER = "log"
    LOG_FILE_NAME = "web_scrapy.log"
    # LOG_FILE_PATH = "%s/%s" % (LOG_FILE_FOLDER, LOG_FILE_NAME)
    LOG_FILE_MAX_SIZE = 10 # Unit: MB
    LOG_FILE_ROTATE_AMOUNT = 10
    BYTES_TO_MEGABYTES_MULTIPLE = 1024 * 1024
    CHECK_TIME_INTERVAL = 300
    LOG_LEVEL_DESCRITION_DEBUG = "DEBUG"
    LOG_LEVEL_DESCRITION_INFO = "INFO"
    LOG_LEVEL_DESCRITION_WARN = "WARN"
    LOG_LEVEL_DESCRITION_ERROR = "ERROR"

    @classmethod
    def __get_rotate_filepath(cls, log_file_path, index):
        # if not os.path.exists(log_file_path):
        #     raise ValueError("The log file[%s] does NOT exist" % log_file_path)
        ret_list = log_file_path.rsplit(".")
        if len(ret_list) != 2:
            raise ValueError("Incorrect log file path: %s" % log_file_path)
        filepath_template = ret_list[0]
        filepath = "%s.tar.gz.%d" % (filepath_template, index)
        return filepath


    @classmethod
    def __get_file_size(cls, log_file_path):
        if not os.path.exists(log_file_path):
            raise ValueError("The log file[%s] does NOT exist" % log_file_path)
        return os.path.getsize(log_file_path)


    @classmethod
    def __reset_log_file_content(cls, log_file_path):
        if not os.path.exists(log_file_path):
            raise ValueError("The log file[%s] does NOT exist" % log_file_path)
        with open(log_file_path, "w") as fp:
            fp.seek(0, 0)


    @classmethod
    def __calculate_rotate_file_count(cls, log_file_path, log_file_rotate_amount):
        log_file_rotate_count = 0
        for index in range(log_file_rotate_amount, 0, -1):
            filepath = cls.__get_rotate_filepath(log_file_path, index)
            if os.path.exists(filepath):
                log_file_rotate_count = index
                break
        return log_file_rotate_count


    @classmethod
    def __need_rotate(cls, log_file_path, rotate_filesize_threshold):
        return True if cls.__get_file_size(log_file_path) >= rotate_filesize_threshold else False


    @classmethod
    def __initiate_logger(cls, log_file_relative_path, log_file_level=logging.DEBUG):
        # import pdb;pdb.set_trace()
    # # Create the folder for log file if it does NOT exist
    #     if not os.path.exists(LOG_FILE_FOLDER):
    #         os.makedirs(LOG_FILE_FOLDER)
        # logging.basicConfig(filename=log_file_relative_path, level=log_file_level, format='%(asctime)-15s %(filename)s:%(lineno)d [%(levelname)s]: %(message)s')
        logging.basicConfig(filename=log_file_relative_path, level=log_file_level, format='%(asctime)-15s %(message)s')
# Log to console
        # console = logging.StreamHandler()
        # console.setLevel(logging.WARN)
        # console.setFormatter(logging.Formatter('%(asctime)-15s %(filename)s:%(lineno)d -- %(message)s'))
        # logging.getLogger().addHandler(console)
    # Log to syslog
        from logging.handlers import SysLogHandler
        syslog = SysLogHandler(address='/dev/log')
        # syslog.setLevel(logging.INFO)
        # syslog.setFormatter(logging.Formatter('%(filename)s:%(lineno)d [%(levelname)s]: %(message)s'))
        logging.getLogger().addHandler(syslog)
        return logging.getLogger();


    @classmethod
    def __format_log_string(cls, log_level_description, *args):
        filename = inspect.stack()[2][1].rsplit("/", 1)[-1]
        lineno = inspect.stack()[2][2]
        log_string = "%s:%d [%s]: " % (filename, lineno, log_level_description)
        if len(args) == 1:
            log_string += args[0]
        else:
            log_string += args[0] % args[1:]
        return log_string


    def __init__(self, **cfg):
        self.xcfg = {
            "log_working_folder": self.LOG_WORKING_FOLDER,
            "log_file_folder": self.LOG_FILE_FOLDER,
            "log_file_name": self.LOG_FILE_NAME,
            # "log_file_path": self.LOG_FILE_PATH,
            "log_file_max_size": self.LOG_FILE_MAX_SIZE,
            "log_file_rotate_amount": self.LOG_FILE_ROTATE_AMOUNT,
            "log_file_level": logging.DEBUG,
            "check_time_interval": self.CHECK_TIME_INTERVAL,
        }
        if cfg is not None:
            for (k, v) in cfg.items():
                if isinstance(v, unicode):
                    config[k] = v.encode(CODING_UTF8)
        self.xcfg.update(cfg)
        self.lock = Lock()
        self.exit = False
        self.log_file_relative_path = "%s/%s" % (self.xcfg["log_file_folder"], self.xcfg["log_file_name"])
        self.log_file_path = "%s/%s" % (self.xcfg["log_working_folder"], self.log_file_relative_path)
# Create log file if the file does NOT exist
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, "a+") as f: pass
        time.sleep(1)
        self.log_file_rotate_count = self.__calculate_rotate_file_count(self.log_file_path, self.xcfg["log_file_rotate_amount"])
        self.log_file_rotate_filesize_threshold = self.xcfg["log_file_max_size"] * self.BYTES_TO_MEGABYTES_MULTIPLE
        self.logger = self.__initiate_logger(self.log_file_relative_path, self.xcfg["log_file_level"])
        super(LogRotate, self).__init__()
        self.daemon = True
        self.start()


    def __rotate_file(self):
        # import pdb; pdb.set_trace()
# Need to delete the oldest file
        if self.log_file_rotate_count == self.LOG_FILE_ROTATE_AMOUNT:
            filepath = self.__get_rotate_filepath(self.log_file_path, self.LOG_FILE_ROTATE_AMOUNT)
            os.remove(filepath)
# Shift the rar file
        rotate_count = ((self.LOG_FILE_ROTATE_AMOUNT - 1) if self.log_file_rotate_count == self.LOG_FILE_ROTATE_AMOUNT else self.log_file_rotate_count)
        for index in range(rotate_count, 0, -1):
            old_filepath = self.__get_rotate_filepath(self.log_file_path, index)
            new_filepath = self.__get_rotate_filepath(self.log_file_path, index + 1)
            os.rename(old_filepath, new_filepath)
# Tar the current file
        tar_filepath = self.__get_rotate_filepath(self.log_file_path, 1)
        with tarfile.open(tar_filepath, "w:gz") as tar:
            os.chdir("%s/%s" % (self.xcfg["log_working_folder"], self.xcfg["log_file_folder"]))
            tar.add(self.xcfg["log_file_name"])
# Add the log count
        if self.log_file_rotate_count < self.LOG_FILE_ROTATE_AMOUNT:
            self.log_file_rotate_count += 1
# Re-generate the log file
        self.__reset_log_file_content(self.log_file_path)


    def run(self):
        while not self.exit:
            if self.__need_rotate(self.log_file_path, self.log_file_rotate_filesize_threshold):
                with self.lock:
                    self.__rotate_file()
            time.sleep(self.xcfg["check_time_interval"])


    def debug(self, *args):
        log_string = self.__format_log_string(self.LOG_LEVEL_DESCRITION_DEBUG, *args)
        with self.lock:
            self.logger.debug(log_string)
            # self.logger.debug(log_string)


    def info(self, *args):
        log_string = self.__format_log_string(self.LOG_LEVEL_DESCRITION_INFO, *args)
        with self.lock:
            self.logger.info(log_string)
            # self.logger.info(*args)


    def warn(self, *args):
        log_string = self.__format_log_string(self.LOG_LEVEL_DESCRITION_WARN, *args)
        with self.lock:
            self.logger.warn(log_string)
            # self.logger.warn(*args)


    def error(self, *args):
        log_string = self.__format_log_string(self.LOG_LEVEL_DESCRITION_ERROR, *args)
        with self.lock:
            self.logger.error(log_string)
            # self.logger.error(*args)
