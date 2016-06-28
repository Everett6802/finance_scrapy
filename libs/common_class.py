from datetime import datetime, timedelta
import math
import common as CMN


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

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        # import pdb; pdb.set_trace()
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            if hasattr(self._instance, "initialize"):
                self._instance.initialize()
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


    def get_year(self):
        assert (self.year is None), "year value should NOT be None"
        return self.year


    def get_republic_era_year(self):
        assert (self.republic_era_year is None), "republic_era_year value should NOT be None"
        return self.republic_era_year


    def setup_year_value(self, year_value):
        if CMN.is_republic_era_year(year_value):
            self.republic_era_year = int(year_value)
            self.year = self.republic_era_year + 1911
        else:
            self.year = int(year_value)
            self.republic_era_year = self.year - 1911


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

    def __init__(self, *args):
        super(FinanceDate, self).__init__()
        self.month = None # range: 1 - 12
        self.day = None # range: 1 - last date of month
        self.date_str = None
        self.datetime_cfg = None
        # import pdb; pdb.set_trace()
        try:
            if len(args) == 1:
                time_cfg = None
                if isinstance(args[0], str):
                    mobj = CMN.check_date_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.month = int(mobj.group(2))
                    self.day = int(mobj.group(3))
                elif isinstance(args[0], datetime):
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
# Check Year Range
        CMN.check_year_range(self.year)
# Check Month Range
        CMN.check_month_range(self.month)
# Check Day Range
        CMN.check_day_range(self.day, self.year,self.month)


    def to_string(self):
        if self.date_str is None:
            self.date_str = CMN.transform_date_str(self.year, self.month, self.day)
        return self.date_str


    def get_value(self):
        return (self.year << 8 | self.month << 4 | self.day)


    def to_datetime(self):
        if self.datetime_cfg is None:
            self.datetime_cfg = datetime(self.year, self.month, self.day)
        return self.datetime_cfg


class FinanceMonth(FinanceTimeBase):

    def __init__(self, *args):
        super(FinanceMonth, self).__init__()
        self.month = None # range: 1 - 12
        self.month_str = None
        try:
            if len(args) == 1:
                time_cfg = None
                if isinstance(args[0], str):
                    mobj = CMN.check_month_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.month = int(mobj.group(2))
                elif isinstance(args[0], datetime):
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
# Check Year Range
        CMN.check_year_range(self.year)
# Check Month Range
        CMN.check_month_range(self.month)


    def to_string(self):
        if self.month_str is None:
            self.month_str = CMN.transform_month_str(self.year, self.month, self.day)
        return self.month_str


    def get_value(self):
        return (self.year << 4 | self.month)


class FinanceQuarter(FinanceTimeBase):

    def __init__(self, *args):
        super(FinanceQuarter, self).__init__()
        self.quarter = None
        self.quarter_str = None
        # import pdb; pdb.set_trace()
        try:
            if len(args) == 1:
                if isinstance(args[0], str):
                    mobj = CMN.check_quarter_str_format(args[0])
                    self.setup_year_value(mobj.group(1))
                    # self.year = mobj.group(1)
                    self.quarter = int(mobj.group(2))
                elif isinstance(args[0], datetime):
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
# Check Year Range
        CMN.check_year_range(self.year)
# Check Quarter Range
        CMN.check_quarter_range(self.quarter)


    def to_string(self):
        if self.quarter_str is None:
            self.quarter_str = CMN.transform_quarter_str(self.year, self.quarter)
        return self.quarter_str


    def get_value(self):
        return (self.year << 3 | self.quarter)

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
