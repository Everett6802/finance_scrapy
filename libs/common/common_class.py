import re
import time
import threading
from datetime import datetime, timedelta
import math
import collections
import common_definition as CMN_DEF
from libs.common.common_variable import GlobalVar as GV
import common_function as CMN_FUNC

MonthTuple = collections.namedtuple('MonthTuple', ('year', 'month'))
QuarterTuple = collections.namedtuple('QuarterTuple', ('year', 'quarter'))
TimeDurationTuple = collections.namedtuple('TimeDurationTuple', ('time_duration_start', 'time_duration_end'))
ScrapyClassTimeDurationTuple = collections.namedtuple('ScrapyClassTimeDurationTuple', ('scrapy_class_index', 'time_duration_type', 'time_duration_start', 'time_duration_end'))
ScrapyClassCompanyTimeDurationTuple = collections.namedtuple('ScrapyClassCompanyTimeDurationTuple', ('scrapy_class_index', 'company_code_number', 'time_duration_type', 'time_duration_start', 'time_duration_end'))

singleton_thread_lock = threading.Lock()


class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the class that should be a singleton.

    The decorated class can define one `__init__` function that takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated


    def Instance(self, cfg=None):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            with singleton_thread_lock:
                try:
                    return self._instance
                except AttributeError:
                    # import pdb; pdb.set_trace()
                    self._instance = self._decorated() # Call __init__() of the class
                    if hasattr(self._instance, "initialize"):
                        if cfg is None:
                            self._instance.initialize()
                        else:
                            self._instance.initialize(**cfg)
        return self._instance


    def __call__(self):
        raise TypeError('Singletons must be accessed through Instance()')


    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)

#############################################################################################

class FinanceTimeBase(object):

    def __init__(self):
        self.year = None
        self.republic_era_year = None
        pass


    def to_string(self):
        raise NotImplementedError


    def get_value(self):
        raise NotImplementedError


    def get_value_tuple(self):
        raise NotImplementedError


    def check_continous_time_duration(self, another_time_duration):
        raise NotImplementedError


    def get_year(self):
        assert (self.year is not None), "year value should NOT be None"
        return self.year


    def get_republic_era_year(self):
        assert (self.republic_era_year is not None), "republic_era_year value should NOT be None"
        return self.republic_era_year


    def setup_year_value(self, year_value):
        if CMN_FUNC.is_republic_era_year(year_value):
            self.republic_era_year = int(year_value)
            self.year = self.republic_era_year + CMN_DEF.REPUBLIC_ERA_YEAR_OFFSET
        else:
            self.year = int(year_value)
            self.republic_era_year = self.year - CMN_DEF.REPUBLIC_ERA_YEAR_OFFSET


    def check_continous_time_duration(self, another_time_duration):
        return CMN_FUNC.is_continous_time_duration(self, another_time_duration)


    @staticmethod
    def get_time_unit_type():
        # """IMPORTANT: This is a static method, override it with @staticmethod !"""
        raise NotImplementedError


    @classmethod
    def from_string(cls, time_string):
        # """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    @staticmethod
    def from_time_string(time_string):
        if CMN_FUNC.is_date_str_format(time_string):
            return FinanceDate.from_string(time_string)
        elif CMN_FUNC.is_month_str_format(time_string):
            return FinanceMonth.from_string(time_string)
        elif CMN_FUNC.is_quarter_str_format(time_string):
            return FinanceQuarter.from_string(time_string)
        else:
            ValueError("Unknown time format: %s" % time_string)


    def __str__(self):
        return self.to_string()


    def __lt__(self, other):
        return self.get_value() < other.get_value()


    def __le__(self, other):
        return self.get_value() <= other.get_value()


    def __eq__(self, other):
        return self.get_value() == other.get_value()


    def __ne__(self, other):
        return self.get_value() != other.get_value()


    def __gt__(self, other):
        return self.get_value() > other.get_value()


    def __ge__(self, other):
        return self.get_value() >= other.get_value()


class FinanceDate(FinanceTimeBase):

    today_finance_date = None
    last_finance_date = None
    def __init__(self, *args):
        super(FinanceDate, self).__init__()
        self.month = None # range: 1 - 12
        self.day = None # range: 1 - last date of month
        self.date_str = None
        self.datetime_cfg = None
        try:
            format_unsupport = False
            if len(args) == 1:
                time_cfg = None
                if isinstance(args[0], str):
                    mobj = CMN_FUNC.check_date_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.month = int(mobj.group(2))
                    self.day = int(mobj.group(3))
                elif isinstance(args[0], datetime) or isinstance(args[0], FinanceDate):
                    self.setup_year_value(args[0].year)
                    # self.year = args[0].year
                    self.month = args[0].month
                    self.day = args[0].day
                else:
                    format_unsupport = True
            elif len(args) == 3:
                for index in range(3):
                    if type(args[index]) is not int:
                        format_unsupport = True
                self.setup_year_value(args[0])
                # self.year = args[0]
                self.month = args[1]
                self.day = args[2]
            else:
                format_unsupport = True
            if format_unsupport:
                raise ValueError("Unsupport argument format: %s" % [type(data) for data in args])
        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception("Exception occurs in FinanceDate, due to: %s" % str(e))
# Check value range
        FinanceDate.check_value_range(self.year, self.month, self.day)


    @staticmethod
    def check_value_range(year, month, day):
# Check Year Range
        CMN_FUNC.check_year_range(year)
# Check Month Range
        CMN_FUNC.check_month_range(month)
# Check Day Range
        CMN_FUNC.check_day_range(day, year, month)


    @staticmethod
    def get_time_unit_type():
        return CMN_DEF.DATA_TIME_UNIT_DAY


    @classmethod
    def from_string(cls, time_string):
        return cls(time_string)


    @classmethod
    def get_today_finance_date(cls):
        if cls.today_finance_date is None:
            cls.today_finance_date = FinanceDate(datetime.today())
        return cls.today_finance_date


    @classmethod
    def get_last_finance_date(cls):
        if cls.last_finance_date is None:
            today_data_exist_hour = CMN_DEF.TODAY_DATA_EXIST_HOUR # if GV.IS_FINANCE_MARKET_MODE else CMN_DEF.TODAY_STOCK_DATA_EXIST_HOUR
            today_data_exist_minute = CMN_DEF.TODAY_DATA_EXIST_MINUTE # if GV.IS_FINANCE_MARKET_MODE else CMN_DEF.TODAY_STOCK_DATA_EXIST_HOUR
            cls.last_finance_date = CMN_FUNC.get_last_url_data_date(today_data_exist_hour, today_data_exist_minute) 
        return cls.last_finance_date


    def __add__(self, day_delta):
        # if not isinstance(delta, timedelta):
        #     raise TypeError('The type[%s] of the other variable is NOT timedelta' % type(delta))
        if not isinstance(day_delta, int):
            raise TypeError('The type[%s] of the day_delta argument is NOT int' % type(day_delta))
        return FinanceDate(self.to_datetime() + timedelta(days = day_delta))


    def __sub__(self, day_delta):
        # if not isinstance(delta, timedelta):
        #     raise TypeError('The type[%s] of the other variable is NOT timedelta' % type(delta))
        if not isinstance(day_delta, int):
            raise TypeError('The type[%s] of the day_delta argument is NOT int' % type(day_delta))
        return FinanceDate(self.to_datetime() - timedelta(days = day_delta))


    def to_string(self):
        if self.date_str is None:
            self.date_str = CMN_FUNC.transform_date_str(self.year, self.month, self.day)
        return self.date_str


    def get_value(self):
        return (self.year << 12 | self.month << 8 | self.day)


    def get_value_tuple(self):
        return (self.year, self.month, self.day)


    def to_datetime(self):
        if self.datetime_cfg is None:
            self.datetime_cfg = datetime(self.year, self.month, self.day)
        return self.datetime_cfg


    @staticmethod
    def is_same_month(finance_date1, finance_date2):
        return (True if FinanceMonth(finance_date1.year, finance_date1.month) == FinanceMonth(finance_date2.year, finance_date2.month) else False)


class FinanceMonth(FinanceTimeBase):

    @classmethod
    def get_finance_month_from_date(cls, *date_args):
        """ Find the finance month due to the specific finance date"""
        
        finance_date = None
        if isinstance(date_args[0], FinanceDate):
            finance_date = date_args[0]
        else:
            finance_date = FinanceDate(*date_args)

        return cls(finance_date.year, finance_date.month)


    def __init__(self, *args):
        super(FinanceMonth, self).__init__()
        self.month = None # range: 1 - 12
        self.month_str = None
        try:
            format_unsupport = False
            if len(args) == 1:
                time_cfg = None
                if isinstance(args[0], str):
                    mobj = CMN_FUNC.check_month_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.month = int(mobj.group(2))
                elif isinstance(args[0], datetime) or isinstance(args[0], FinanceMonth):
                    self.setup_year_value(args[0].year)
                    # self.year = args[0].year
                    self.month = args[0].month
                else:
                    format_unsupport = True
            elif len(args) == 2:
                for index in range(2):
                    if type(args[index]) is not int:
                        format_unsupport = True
                self.setup_year_value(args[0])
                # self.year = args[0]
                self.month = args[1]
            else:
                format_unsupport = True
            if format_unsupport:
                raise ValueError("Unsupport argument format: %s" % [type(data) for data in args])
        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception("Exception occurs in FinanceMonth, due to: %s" % str(e))
# Check value range
        FinanceMonth.check_value_range(self.year, self.month)


    @staticmethod
    def check_value_range(year, month):
# Check Year Range
        CMN_FUNC.check_year_range(year)
# Check Month Range
        CMN_FUNC.check_month_range(month)


    @staticmethod
    def get_time_unit_type():
        return CMN_DEF.DATA_TIME_UNIT_MONTH


    @classmethod
    def from_string(cls, time_string):
        return cls(time_string)


    def __to_month_index(self):
        return self.year * 12 + self.month - 1


    def __from_month_index_to_value(self, month_index):
        # year = month_index / 12
        # month = month_index % 12 + 1
        return MonthTuple(month_index / 12, month_index % 12 + 1)


    def __add__(self, month_delta):
        if not isinstance(month_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(month_delta))

        new_month_index = self.__to_month_index() + month_delta
        new_month_tuple = self.__from_month_index_to_value(new_month_index)
        return FinanceMonth(new_month_tuple.year, new_month_tuple.month)


    def __sub__(self, month_delta):
        if not isinstance(month_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(month_delta))

        new_month_index = self.__to_month_index() - month_delta
        new_month_tuple = self.__from_month_index_to_value(new_month_index)
        return FinanceMonth(new_month_tuple.year, new_month_tuple.month)


    def to_string(self):
        if self.month_str is None:
            self.month_str = CMN_FUNC.transform_month_str(self.year, self.month)
        return self.month_str


    def get_value(self):
        return (self.year << 4 | self.month)


    def get_value_tuple(self):
        return (self.year, self.month)


class FinanceQuarter(FinanceTimeBase):

    ANNUAL_REPORT_MONTH = 3
    ANNUAL_REPORT_DAY = 31
    Q1_QUARTERLY_REPORT_MONTH = 5
    Q1_QUARTERLY_REPORT_DAY = 15
    Q2_QUARTERLY_REPORT_MONTH = 8
    Q2_QUARTERLY_REPORT_DAY = 14
    Q3_QUARTERLY_REPORT_MONTH = 11
    Q3_QUARTERLY_REPORT_DAY = 14

    @classmethod
    def __get_statement_release_date_list(cls, year):
        statement_release_date_list = [
            FinanceDate(year, cls.ANNUAL_REPORT_MONTH, cls.ANNUAL_REPORT_DAY),
            FinanceDate(year, cls.Q1_QUARTERLY_REPORT_MONTH, cls.Q1_QUARTERLY_REPORT_DAY),
            FinanceDate(year, cls.Q2_QUARTERLY_REPORT_MONTH, cls.Q2_QUARTERLY_REPORT_DAY),
            FinanceDate(year, cls.Q3_QUARTERLY_REPORT_MONTH, cls.Q3_QUARTERLY_REPORT_DAY),            
        ]
        return statement_release_date_list


    @classmethod
    def get_start_finance_quarter_from_date(cls, *date_args):
        """ Find the nearest start finance qaurter due to the specific finance date"""
        finance_date = None
        if isinstance(date_args[0], FinanceDate):
            finance_date = date_args[0]
        else:
            finance_date = FinanceDate(*date_args)
        statement_release_date_list = cls.__get_statement_release_date_list(finance_date.year)
        finance_quarter = None
        if finance_date <= statement_release_date_list[0]:
            finance_quarter = FinanceQuarter(finance_date.year - 1, 4)
        elif statement_release_date_list[1] >= finance_date > statement_release_date_list[0]:
            finance_quarter = FinanceQuarter(finance_date.year, 1)
        elif statement_release_date_list[2] >= finance_date > statement_release_date_list[1]:
            finance_quarter = FinanceQuarter(finance_date.year, 2)
        elif statement_release_date_list[3] >= finance_date > statement_release_date_list[2]:
            finance_quarter = FinanceQuarter(finance_date.year, 3)
        elif finance_date >= statement_release_date_list[3]:
            finance_quarter = FinanceQuarter(finance_date.year, 4)
        else:
            raise ValueError("Fail to transform the finance date[%s] to quarter" % finance_date)
        return finance_quarter


    @classmethod
    def get_end_finance_quarter_from_date(cls, *date_args):
        """ Find the nearest end finance qaurter due to the specific finance date"""
        finance_date = None
        if isinstance(date_args[0], FinanceDate):
            finance_date = date_args[0]
        else:
            finance_date = FinanceDate(*date_args)
        statement_release_date_list = cls.__get_statement_release_date_list(finance_date.year)
        finance_quarter = None
        if finance_date < statement_release_date_list[0]:
            finance_quarter = FinanceQuarter(finance_date.year - 1, 3)
        elif statement_release_date_list[1] > finance_date >= statement_release_date_list[0]:
            finance_quarter = FinanceQuarter(finance_date.year - 1, 4)
        elif statement_release_date_list[2] > finance_date >= statement_release_date_list[1]:
            finance_quarter = FinanceQuarter(finance_date.year, 1)
        elif statement_release_date_list[3] > finance_date >= statement_release_date_list[2]:
            finance_quarter = FinanceQuarter(finance_date.year, 2)
        elif finance_date >= statement_release_date_list[3]:
            finance_quarter = FinanceQuarter(finance_date.year, 3)
        else:
            raise ValueError("Fail to transform the end finance date[%s] to quarter" % finance_date)
        return finance_quarter


    def __init__(self, *args):
        super(FinanceQuarter, self).__init__()
        self.quarter = None
        self.quarter_str = None
        # import pdb; pdb.set_trace()
        try:
            format_unsupport = False
            if len(args) == 1:
                if isinstance(args[0], str):
                    mobj = CMN_FUNC.check_quarter_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.quarter = int(mobj.group(2))
                elif isinstance(args[0], datetime) or isinstance(args[0], FinanceQuarter):
                    self.setup_year_value(args[0].year)
                    # self.year = args[0].year
                    self.quarter = (int)(math.ceil(args[0].month / 3.0))
                else:
                    format_unsupport = True
            elif len(args) == 2:
                for index in range(2):
                    if type(args[index]) is not int:
                        format_unsupport = True
                self.year = args[0]
                self.quarter = args[1]
            else:
                format_unsupport = True
            if format_unsupport:
                raise ValueError("Unsupport argument format: %s" % [type(data) for data in args])
        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception("Exception occurs in FinanceQuarter, due to: %s" % str(e))
# Check value Range
        FinanceQuarter.check_value_range(self.year, self.quarter)


    @staticmethod
    def check_value_range(year, quarter):
# Check Year Range
        CMN_FUNC.check_year_range(year)
# Check Quarter Range
        CMN_FUNC.check_quarter_range(quarter)


    @staticmethod
    def get_time_unit_type():
        return CMN_DEF.DATA_TIME_UNIT_QUARTER


    @classmethod
    def from_string(cls, time_string):
        return cls(time_string)


    def __to_quarter_index(self):
        return self.year * 4 + self.quarter - 1


    def __from_quarter_index_to_value(self, quarter_index):
        return QuarterTuple(quarter_index / 4, quarter_index % 4 + 1)


    def __add__(self, quarter_delta):
        if not isinstance(quarter_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(quarter_delta))

        new_quarter_index = self.__to_quarter_index() + quarter_delta
        new_quarter_tuple = self.__from_quarter_index_to_value(new_quarter_index)
        return FinanceQuarter(new_quarter_tuple.year, new_quarter_tuple.quarter)


    def __sub__(self, quarter_delta):
        if not isinstance(quarter_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(quarter_delta))

        new_quarter_index = self.__to_quarter_index() - quarter_delta
        new_quarter_tuple = self.__from_quarter_index_to_value(new_quarter_index)
        return FinanceQuarter(new_quarter_tuple.year, new_quarter_tuple.quarter)


    def to_string(self):
        if self.quarter_str is None:
            self.quarter_str = CMN_FUNC.transform_quarter_str(self.year, self.quarter)
        return self.quarter_str


    def get_value(self):
        return (self.year << 3 | self.quarter)


    def get_value_tuple(self):
        return (self.year, self.quarter)


class FinanceYear(FinanceTimeBase):

    @classmethod
    def get_finance_year_from_date(cls, *date_args):
        """ Find the finance year due to the specific finance date"""
        
        finance_date = None
        if isinstance(date_args[0], FinanceDate):
            finance_date = date_args[0]
        else:
            raise ValueError("UnSupport input argument: %s" % date_args)
        return cls(finance_date.year)


    def __init__(self, *args):
        super(FinanceYear, self).__init__()
        self.year = None # range: 2000 - 2099
        self.year_str = None
        # import pdb; pdb.set_trace()
        try:
            format_unsupport = False
            if len(args) == 1:
                time_cfg = None
                if isinstance(args[0], str):
                    mobj = CMN_FUNC.check_year_str_format(args[0])
                    self.setup_year_value(mobj.group(0))
                elif isinstance(args[0], datetime) or isinstance(args[0], FinanceMonth):
                    self.setup_year_value(args[0].year)
                else:
                    format_unsupport = True
            else:
                format_unsupport = True
            if format_unsupport:
                raise ValueError("Unsupport argument format: %s" % [type(data) for data in args])
        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception("Exception occurs in FinanceYear, due to: %s" % str(e))
# Check value range
        CMN_FUNC.check_year_range(self.year)


    @staticmethod
    def get_time_unit_type():
        return CMN_DEF.DATA_TIME_UNIT_YEAR


    @classmethod
    def from_string(cls, time_string):
        return cls(time_string)


    def __add__(self, year_delta):
        if not isinstance(year_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(year_delta))
        new_year = self.year + year_delta
        return FinanceYear(year)


    def __sub__(self, year_delta):
        if not isinstance(year_delta, int):
            raise TypeError('The type[%s] of the delta argument is NOT int' % type(year_delta))
        new_year = self.year - year_delta
        return FinanceYear(year)


    def to_string(self):
        if self.year_str is None:
            self.year_str = "%d" % self.year
        return self.year_str


    def get_value(self):
        return self.year


    def get_value_tuple(self):
        return (self.year,)


class FinanceTimeRange(object):

    def __init__(self, *args):
        self.time_start = None
        self.time_end = None
        self.time_range_str = None
        # import pdb; pdb.set_trace()
        try:
            format_unsupport = False
            if len(args) == 1:
                if isinstance(args[0], str):
                    (self.time_start, self.time_end) = CMN_FUNC.parse_time_duration_range_str_to_object(args[0])
                else:
                    format_unsupport = True
            elif len(args) == 2:
                for index in range(2):
                    if not isinstance(args[index], FinanceTimeBase):
                        format_unsupport = True
                self.time_start = args[0]
                self.time_end = args[1]
            else:
                format_unsupport = True
            if format_unsupport:
                raise ValueError("Unsupport argument format: %s" % [type(data) for data in args])
        except ValueError as e:
            raise e
        except Exception as e:
            raise Exception("Exception occurs in FinanceTimeRange, due to: %s" % str(e))


    def is_greater_than_time_start(self, finance_time):
        return False if ((self.time_start is not None) and (finance_time < self.time_start)) else True


    def is_less_than_time_end(self, finance_time):
        return False if ((self.time_end is not None) and (finance_time > self.time_end)) else True


# class ParseURLDataType:

#     def __init__(self):
#         # self.parse_url_data_type = None
#         pass


#     def get_type(self):
#         raise NotImplementedError


# class ParseURLDataByBS4(ParseURLDataType):

#     def __init__(self, encoding, select_flag):
#         # self.parse_url_data_type = CMN.PARSE_URL_DATA_BY_BS4
#         self.encoding = encoding
#         self.select_flag = select_flag


#     def get_type(self):
#         return CMN.PARSE_URL_DATA_BY_BS4


# class ParseURLDataByJSON(ParseURLDataType):

#     def __init__(self, data_field_name):
#         # self.parse_url_data_type = CMN.PARSE_URL_DATA_BY_BS4
#         self.data_field_name = data_field_name


#     def get_type(self):
#         return CMN.PARSE_URL_DATA_BY_JSON


class FinanceTimerThread(threading.Thread):

    def __init__(self, **cfg):
        super(FinanceTimerThread, self).__init__()
        self.daemon = True
        self.xcfg = {
            "func_ptr": None,
            "interval": 30,
        }
        self.xcfg.update(cfg)
        # self.exit = False
        # if self.xcfg["func_ptr"] is None:
        #     raise ValueError("func_ptr should NOT be None")
        self.exit_event = threading.Event()
        self.interval = self.xcfg["interval"]
        self.func_ptr = None
        self.func_args = None
        self.func_kwargs = None
        self.start_time = None


    def start_timer(self, func_ptr, *args, **kwargs):
        self.func_ptr = func_ptr
        self.func_args = args
        self.func_kwargs = kwargs
        # self.start_time = time()
        # self.exit = True
        self.start()


    def stop_timer(self, timeout=5):
        # self.exit = True
        self.exit_event.set( )
        threading.Thread.join(self, timeout)


    def run(self):
        while not self.exit_event.isSet( ):
            self.func_ptr(*self.func_args, **self.func_kwargs)
            self.exit_event.wait(self.interval)


#############################################################################################

class CSVTimeRangeUpdate(object):
        
    CSV_APPEND_NONE = 0 # No new web data to append
    CSV_APPEND_BEFORE = 1 #  new web data will be appended in front of the old csv data
    CSV_APPEND_AFTER = 2 #  new web data will be appended in back of the old csv data
        # CSV_APPEND_BOTH = 3 #  new web data will be appended in front and back(both) of the old csv data

    @classmethod
    def get_init_csv_time_duration_update(cls, time_duration_start, time_duration_end):
        # import pdb; pdb.set_trace()
# If it's time first time to write the data from web to CSV ......
        web2csv_time_duration_update = cls()
        web2csv_time_duration_update.NewCSVStart = web2csv_time_duration_update.NewWebStart = time_duration_start
        web2csv_time_duration_update.NewCSVEnd = web2csv_time_duration_update.NewWebEnd = time_duration_end
        web2csv_time_duration_update.AppendDirection = cls.CSV_APPEND_AFTER
        new_csv_extension_time_duration = TimeDurationTuple(web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd)
        return (new_csv_extension_time_duration, (web2csv_time_duration_update,),)


    @classmethod
    def get_extended_csv_time_duration_update(cls, time_duration_start, time_duration_end, csv_old_time_duration_tuple):
        # import pdb; pdb.set_trace()
# Adjust the time duration, ignore the data which already exist in the finance data folder
# I assume that the time duration between the csv data and new data should be consecutive
# Two cases which the original time range can be extended successfully: 
# (1) The new time range overlaps the original one
# (2) The new time range fully covers the original one
        overlap_case = CMN_FUNC.get_time_range_overlap_case(time_duration_start, time_duration_end, csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end)
        new_csv_extension_time_duration = None
        web2csv_time_duration_update_before = None
        web2csv_time_duration_update_after = None
        if overlap_case == CMN_DEF.TIME_OVERLAP_COVERED:
# # All csv data already exists, no need to update the new data
#             g_logger.debug("The time duration[%s:%s] of the CSV data[%s] already exist ......" % (time_duration_start, time_duration_end, CMN_DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX]))
#             new_csv_extension_time_duration = None
#             return None
            return (new_csv_extension_time_duration, None,)
        elif overlap_case == CMN_DEF.TIME_OVERLAP_BEFORE:
# The new time range is extended before the start side of the original time range
            web2csv_time_duration_update_before = cls()
            web2csv_time_duration_update_before.OldCSVStart = csv_old_time_duration_tuple.time_duration_start
            web2csv_time_duration_update_before.OldCSVEnd = csv_old_time_duration_tuple.time_duration_end
            web2csv_time_duration_update_before.NewWebStart = time_duration_start
            web2csv_time_duration_update_before.NewWebEnd = web2csv_time_duration_update_before.OldCSVStart - 1
            web2csv_time_duration_update_before.AppendDirection = cls.CSV_APPEND_BEFORE
            # g_logger.debug("Extend the time duration before the original CSV data[%s %s:%s]: %s:%s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], web2csv_time_duration_update_before.OldCSVStart, web2csv_time_duration_update_before.OldCSVEnd, web2csv_time_duration_update_before.NewWebStart, web2csv_time_duration_update_before.NewWebEnd))
            new_csv_extension_time_duration = TimeDurationTuple(web2csv_time_duration_update_before.NewWebStart, web2csv_time_duration_update_before.OldCSVEnd)
            return (new_csv_extension_time_duration, (web2csv_time_duration_update_before,),)
        elif overlap_case == CMN_DEF.TIME_OVERLAP_AFTER:
# The new time range is extended after the end side of the original time range
            web2csv_time_duration_update_after = cls()
            web2csv_time_duration_update_after.OldCSVStart = csv_old_time_duration_tuple.time_duration_start
            web2csv_time_duration_update_after.OldCSVEnd = csv_old_time_duration_tuple.time_duration_end
            web2csv_time_duration_update_after.NewWebStart = web2csv_time_duration_update_after.OldCSVEnd + 1
            web2csv_time_duration_update_after.NewWebEnd = time_duration_end
            web2csv_time_duration_update_after.AppendDirection = cls.CSV_APPEND_AFTER
            # g_logger.debug("Extend the time duration after the original CSV data[%s %s:%s]: %s:%s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], web2csv_time_duration_update_after.OldCSVStart, web2csv_time_duration_update_after.OldCSVEnd, web2csv_time_duration_update_after.NewWebStart, web2csv_time_duration_update_after.NewWebEnd))
            new_csv_extension_time_duration = TimeDurationTuple(web2csv_time_duration_update_after.OldCSVStart, web2csv_time_duration_update_after.NewWebEnd)
            return (new_csv_extension_time_duration, (web2csv_time_duration_update_after,),)
        elif overlap_case == CMN_DEF.TIME_OVERLAP_COVER:
# The new time range covers the original time range and extended before/after the start/end side of the original time range
            web2csv_time_duration_update_before = cls()
            web2csv_time_duration_update_before.OldCSVStart = csv_old_time_duration_tuple.time_duration_start
            web2csv_time_duration_update_before.OldCSVEnd = csv_old_time_duration_tuple.time_duration_end
            web2csv_time_duration_update_before.NewWebStart = time_duration_start
            web2csv_time_duration_update_before.NewWebEnd = web2csv_time_duration_update_before.OldCSVStart - 1
            web2csv_time_duration_update_before.AppendDirection = cls.CSV_APPEND_BEFORE
            # g_logger.debug("Extend the time duration before the original CSV data[%s %s:%s]: %s:%s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], web2csv_time_duration_update_before.OldCSVStart, web2csv_time_duration_update_before.OldCSVEnd, web2csv_time_duration_update_before.NewWebStart, web2csv_time_duration_update_before.NewWebEnd))
            web2csv_time_duration_update_after = cls()
            web2csv_time_duration_update_after.OldCSVStart = csv_old_time_duration_tuple.time_duration_start
            web2csv_time_duration_update_after.OldCSVEnd = csv_old_time_duration_tuple.time_duration_end
            web2csv_time_duration_update_after.NewWebStart = web2csv_time_duration_update_after.OldCSVEnd + 1
            web2csv_time_duration_update_after.NewWebEnd = time_duration_end
            web2csv_time_duration_update_after.AppendDirection = cls.CSV_APPEND_AFTER
            # g_logger.debug("Extend the time duration after the original CSV data[%s %s:%s]: %s:%s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], web2csv_time_duration_update_after.OldCSVStart, web2csv_time_duration_update_after.OldCSVEnd, web2csv_time_duration_update_after.NewWebStart, web2csv_time_duration_update_after.NewWebEnd))
            new_csv_extension_time_duration = TimeDurationTuple(web2csv_time_duration_update_before.NewWebStart, web2csv_time_duration_update_after.NewWebEnd)
            return (new_csv_extension_time_duration, (web2csv_time_duration_update_before, web2csv_time_duration_update_after,),)
# If the time range of new data contain all the time range of csv data, the system is not desiged to update two time range interval
        else:
            raise CMN.EXCEPTION.WebScrapyUnDefiedCaseException("The system does NOT support this type[2] of the range update; CSV data[%s:%s], new data[%s:%s]" % (csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))


    @classmethod
    def get_csv_time_duration_update(cls, time_duration_start, time_duration_end, csv_old_time_duration_tuple=None):
        if csv_old_time_duration_tuple is None:
            return cls.get_init_csv_time_duration_update(time_duration_start, time_duration_end)
        else:
            return cls.get_extended_csv_time_duration_update(time_duration_start, time_duration_end, csv_old_time_duration_tuple)


    def __init__(self):
        self.append_direction = self.CSV_APPEND_NONE
        self.old_csv_start = None
        self.old_csv_end = None
        self.new_web_start = None
        self.new_web_end = None
        # self.new_csv_start = None
        # self.new_csv_end = None
        self.description = None


    def __str__(self):
        if self.description is None:
            self.description = ""
            if self.old_csv_start is not None:
                self.description += "OCS: %s; " % self.old_csv_start
            if self.old_csv_end is not None:
                self.description += "OCE: %s; " % self.old_csv_end
            if self.new_web_start is not None:
                self.description += "NWS: %s; " % self.new_web_start
            if self.new_web_end is not None:
                self.description += "NWE: %s; " % self.new_web_end
            # if self.new_csv_start is not None:
            #     self.description += "NCS: %s; " % self.new_csv_start
            # if self.new_csv_end is not None:
            #     self.description += "NCE: %s; " % self.new_csv_end
        return self.description


    def __repr__(self):
        return self.__str__()

    @property
    def NeedUpdate(self):
        return (True if (self.append_direction != self.CSV_APPEND_NONE) else False)

    @property
    def AppendDirection(self):
        return self.append_direction
    @AppendDirection.setter
    def AppendDirection(self, append_direction):
        self.append_direction = append_direction

    @property
    def OldCSVStart(self):
        return self.old_csv_start
    @OldCSVStart.setter
    def OldCSVStart(self, old_csv_start):
        self.old_csv_start = old_csv_start

    @property
    def OldCSVEnd(self):
        return self.old_csv_end
    @OldCSVEnd.setter
    def OldCSVEnd(self, old_csv_end):
        self.old_csv_end = old_csv_end

    @property
    def NewWebStart(self):
        return self.new_web_start
    @NewWebStart.setter
    def NewWebStart(self, new_web_start):
        self.new_web_start = new_web_start

    @property
    def NewWebEnd(self):
        return self.new_web_end
    @NewWebEnd.setter
    def NewWebEnd(self, new_web_end):
        self.new_web_end = new_web_end

    # @property
    # def NewCSVStart(self):
    #     return self.new_csv_start
    # @NewCSVStart.setter
    # def NewCSVStart(self, new_csv_start):
    #     self.new_csv_start = new_csv_start

    # @property
    # def NewCSVEnd(self):
    #     return self.new_csv_end
    # @NewCSVEnd.setter
    # def NewCSVEnd(self, new_csv_end):
    #     self.new_csv_end = new_csv_end


    def backup_old_csv_if_necessary(self, csv_filepath, ignore_old_csv_exist=False):
        backup_old_csv = False
        if self.append_direction == self.CSV_APPEND_BEFORE: #BASE.BASE.ScrapyBase.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
            old_csv_filepath = csv_filepath + ".old"
            if CMN_FUNC.check_file_exist(old_csv_filepath):
                if not ignore_old_csv_exist:
                    raise ValueError("The CSV file[%s] already exists !!!" % old_csv_filepath)
            else:
                # g_logger.debug("Need add the new data in front of the old CSV data, rename the file: %s" % (csv_filepath + ".old"))
                CMN_FUNC.rename_file_if_exist(csv_filepath, csv_filepath + ".old") 
                backup_old_csv = True
        return backup_old_csv


    def append_old_csv_if_necessary(self, csv_filepath):
        if self.append_direction == self.CSV_APPEND_BEFORE: #BASE.BASE.ScrapyBase.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
            # g_logger.debug("Append the old CSV data to the file: %s" % csv_filepath)
            CMN_FUNC.append_data_into_file(csv_filepath + ".old", csv_filepath)
            CMN_FUNC.remove_file_if_exist(csv_filepath + ".old") 


class CSVFileNoScrapyRecord(object):

    # STATUS_RECORD_TIME_RANGE_NOT_OVERLAP = 0
    # STATUS_RECORD_CSV_FILE_ALREADY_EXIST = 1
    # STATUS_RECORD_WEB_DATA_NOT_FOUND = 2
    # RECORD_TYPE_INDEX_LIST = [
    #     STATUS_RECORD_TIME_RANGE_NOT_OVERLAP,
    #     STATUS_RECORD_CSV_FILE_ALREADY_EXIST,
    #     STATUS_RECORD_WEB_DATA_NOT_FOUND
    # ]
    RECORD_TYPE_INDEX = 0
    RECORD_TYPE_DESCRIPTION_INDEX = 1
    RECORD_TYPE_ENTRY_LIST = [
        ["TimeRangeNotOverlap", "The search time range does NOT overlap the one in the URL time range lookup table",],
        ["CSVFileAlreadyExist", "The CSV files of the time range has already existed in the local folder",],
        ["WebDataNotFound", "The web data of the URL is NOT found",],
    ]
    RECORD_TYPE_SIZE = len(RECORD_TYPE_ENTRY_LIST)
    TIME_RANGE_NOT_OVERLAP_RECORD_INDEX = 0
    CSV_FILE_ALREADY_EXIST_RECORD_INDEX = 1
    WEB_DATA_NOT_FOUND_RECORD_INDEX = 2

    RECORD_TYPE_LIST = [entry[RECORD_TYPE_INDEX] for entry in RECORD_TYPE_ENTRY_LIST]
    RECORD_TYPE_DESCRIPTION_LIST = [entry[RECORD_TYPE_DESCRIPTION_INDEX] for entry in RECORD_TYPE_ENTRY_LIST]

    @classmethod
    def create_register_status_instance(cls):
        # import pdb; pdb.set_trace()
        csv_file_no_scrapy_record = cls()
        for index in range(cls.RECORD_TYPE_SIZE):
            csv_file_no_scrapy_record.__register_record_type(
                cls.RECORD_TYPE_LIST[index], 
                cls.RECORD_TYPE_DESCRIPTION_LIST[index]
            )
        return csv_file_no_scrapy_record


    def __init__(self):
        self.record_type_dict = {}
        self.record_type_description_dict = {}
        self.web_data_not_found_time_start = None
        self.web_data_not_found_time_end = None


    def __register_record_type(self, record_type_name, record_type_description):
        # import pdb; pdb.set_trace()
        if self.record_type_dict.has_key(record_type_name):
            g_logger.debug("The type[%s] has already exist" % record_type_name)
            return
        self.record_type_dict[record_type_name] = []
        self.record_type_description_dict[record_type_name] = record_type_description


    def __add_record(self, record_type_name, *args):
        if not self.record_type_dict.has_key(record_type_name):
            raise ValueError("Unknown Check Status Type: %s" % record_type_name)
        self.record_type_dict[record_type_name].append(args)


    def add_time_range_not_overlap_record(self, *args):
# Market
# args[0]: source type index
# Stock
# args[0]: source type index
# args[1]: company code number
        self.__add_record("TimeRangeNotOverlap", *args)


    def add_csv_file_already_exist_record(self, *args):
# Market
# args[0]: source type index
# Stock
# args[0]: source type index
# args[1]: company code number
        self.__add_record("CSVFileAlreadyExist", *args)


    def add_web_data_not_found_record(self, *args):
# Market
# args[0]: time slice. None for a must to flush data into list
# args[1]: source type index
# Stock
# args[0]: time slice. None for a must to flush data into list
# args[1]: source type index
# args[2]: company code number
        need_flush = False
        if args[0] is None:
            if self.web_data_not_found_time_start is not None:
                need_flush = True
        else:
            if self.web_data_not_found_time_start is None:
                self.web_data_not_found_time_start = self.web_data_not_found_time_end = args[0]
            else:
                if self.web_data_not_found_time_end.check_continous_time_duration(args[0]):
                    self.web_data_not_found_time_end = args[0]
                else:
                    need_flush = True
# Keep track of the time range in which the web data is empty
        if need_flush:
# Market
# args_new[0]: time slice. None for a must to flush data into list
# args_new[1]: source type index
# args_new[2]: empty time start
# args_new[3]: empty time end
# Stock
# args_new[0]: time slice. None for a must to flush data into list
# args_new[1]: source type index
# args_new[2]: company code number
# args_new[2]: empty time start
# args_new[3]: empty time end
                # import pdb; pdb.set_trace()
                # args_new = copy.deepcopy(args)
            args_new = [arg for arg in args]
            args_new.append(self.web_data_not_found_time_start)
            args_new.append(self.web_data_not_found_time_end)
            self.web_data_not_found_time_start = self.web_data_not_found_time_end = None
            self.__add_record("WebDataNotFound", *args_new)
