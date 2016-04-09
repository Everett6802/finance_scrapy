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
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


class ParseURLDataType:

    def __init__(self):
        # self.parse_url_data_type = None
        pass


    def get_type(self):
        raise NotImplementedError


class ParseURLDataByBS4(ParseURLDataType):

    def __init__(self, encoding, select_flag):
        # self.parse_url_data_type = CMN.PARSE_URL_DATA_BY_BS4
        self.encoding = encoding
        self.select_flag = select_flag


    def get_type(self):
        return CMN.PARSE_URL_DATA_BY_BS4


class ParseURLDataByJSON(ParseURLDataType):

    def __init__(self, .data_field_name):
        # self.parse_url_data_type = CMN.PARSE_URL_DATA_BY_BS4
        self.data_field_name = data_field_name


    def get_type(self):
        return CMN.PARSE_URL_DATA_BY_JSON
