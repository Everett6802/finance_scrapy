import re
import time
import threading
from datetime import datetime, timedelta
import math
import collections
MonthTuple = collections.namedtuple('MonthTuple', ('year', 'month'))
QuarterTuple = collections.namedtuple('QuarterTuple', ('year', 'quarter'))
TimeDurationTuple = collections.namedtuple('TimeDurationTuple', ('time_duration_start', 'time_duration_end'))
ScrapyClassTimeDurationTuple = collections.namedtuple('ScrapyClassTimeDurationTuple', ('scrapy_class_index', 'time_duration_type', 'time_duration_start', 'time_duration_end'))
ScrapyClassCompanyTimeDurationTuple = collections.namedtuple('ScrapyClassCompanyTimeDurationTuple', ('scrapy_class_index', 'company_code_number', 'time_duration_type', 'time_duration_start', 'time_duration_end'))
import common_definition as CMN_DEF
import common_function as CMN_FUNC

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
                    raise
            elif len(args) == 3:
                for index in range(3):
                    if type(args[index]) is not int:
                        raise
                self.setup_year_value(args[0])
                # self.year = args[0]
                self.month = args[1]
                self.day = args[2]
            else:
                raise
        except Exception:
            raise ValueError("Unknown argument in FormatDate format: %s" % args)
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
            today_data_exist_hour = CMN_DEF.TODAY_MARKET_DATA_EXIST_HOUR if CMN_DEF.IS_FINANCE_MARKET_MODE else CMN_DEF.TODAY_STOCK_DATA_EXIST_HOUR
            today_data_exist_minute = CMN_DEF.TODAY_MARKET_DATA_EXIST_MINUTE if CMN_DEF.IS_FINANCE_MARKET_MODE else CMN_DEF.TODAY_STOCK_DATA_EXIST_HOUR
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
                    raise
            elif len(args) == 2:
                for index in range(2):
                    if type(args[index]) is not int:
                        raise
                self.setup_year_value(args[0])
                # self.year = args[0]
                self.month = args[1]
            else:
                raise
        except Exception:
            raise ValueError("Unknown argument in FormatMonth format: %s" % args)
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
                    raise
            elif len(args) == 2:
                for index in range(2):
                    if type(args[index]) is not int:
                        raise
                self.year = args[0]
                self.quarter = args[1]
            else:
                raise
        except Exception:
            raise ValueError("Unknown argument in FormatQuarter format: %s" % args)
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
