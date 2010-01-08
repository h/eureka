from itertools import groupby

# {{{ functions related to short_repr
max_len = 66
def short_repr(item, max_len=max_len):
    '''
    We shorten any string that is longer than ``max_len`` characters by
    insterting '...' in the middle.

    Also accepts lists and dictionaries, which are passed through
    short_list_repr and short_dict_repr, respectively

    '''

    # perhaps we should be calling short_list_repr in stead!
    if isinstance(item, tuple) or isinstance(item, list):
        return short_list_repr(item, max_len)
    elif isinstance(item, dict):
        return short_dict_repr(item, max_len)
    else:
        return short_string_repr(item, max_len)

def short_string_repr(string, max_len=max_len):
    '''
    Handles any type that short_list_repr and short_dict_repr can't handle.

    '''

    if not isinstance(string, basestring):
        string = repr(string)
    if len(string) > max_len:
        left_len = (max_len - 3) / 2
        right_len = max_len - left_len - 3
        string = '%s...%s' % (string[:left_len], string[-right_len:])
    return string

def short_list_repr(lst, max_len=max_len):
    '''
    returns the __repr__ of a dict, where each of the entries is first passed
    through ``short_repr``.

    '''

    return lst.__class__(short_repr(elem, max_len) for elem in lst)

def short_dict_repr(dictionary, max_len=max_len):
    '''
    returns the __repr__ of a dict, where each of the entries is first passed
    through ``short_repr``.

    '''

    result = {}
    if not hasattr(dictionary, 'iteritems'):
        raise AttributeError('Expected dictionary! '
                             'Instead I got something of type %s: %s' %
                             (dictionary.__class__, short_repr(dictionary)))
    for key, value in dictionary.iteritems():
        result[short_repr(key, max_len)] = short_repr(value, max_len)
    return result
# }}} functions related to short_repr

def split_by_comma(string):
    '''
    Splits a string apart by commas, making a list.  Also splits apart ranges:
    1-4 becomes 1, 2, 3, 4

    '''

    lst = string.split(',')
    lst_new = []
    for element in lst:
        if element.count('-'):
            start, end = element.split('-')
            if not start.isdigit() or not end.isdigit():
                return False
            lst_range = range(int(start), int(end))
            lst_range = [str(x) for x in lst_range]
            lst_new += lst_range
        else:
          lst_new.append(element)
    return lst_new

def urldecode(string):
    ''' opposite of urllib.urlencode '''

    import urllib

    return tuple(tuple(urllib.unquote(item)
                       for item in entry.split('=', 1))
                 for entry in string.split('&'))

def chain(iterable):
    '''
    like ``itertools.chain``, but takes an iterable argument, in stead of
    a variable list of arguments.

    '''

    for i in iterable:
        for j in i:
            yield j

def pipe(func1, *args, **kwargs):
    '''
    >>> @pipe(func1)
    >>> def func2(*args):
            ...

    is equivalent to:

    >>> def func2(*args):
    >>>     ...
    >>> func2 = lambda *args: func1(func2(*args))

    '''

    def decorator(func2):
        def decorate(*args_, **kwargs_):
            return func1(func2(*args_, **kwargs_), *args, **kwargs)
        decorate.__name__ = func2.__name__
        decorate.__doc__ = func2.__doc__
        decorate.__dict__ = func2.__dict__
        return decorate
    return decorator

def get_source_path(__file__):
    '''
    Given ``module.__file__``, this function returns the file path of the
    source file for that module.

    '''

    import re, os

    if re.search(r'\.py[co]?$', __file__):
        return re.sub(r'\.py[co]$', '.py', __file__)
    else:
        return '%s%s__init__.py' % (__file__, os.sep)

class IDict(dict):
    '''
    Dictionary which ignores the case of its keys. Should be considered
    immutable.

    Must be constructed with a list of two-tuples, eg:
        IDict([('key1', 'value1'), ('key2', 'value2'), ...])

    IDict.items() returns the items in the original order specified.

    '''

    def __init__(self, item_list):
        lowercase_items = tuple((k.lower(), v) for k,v in item_list)
        super(IDict, self).__init__(lowercase_items)
        self._items = lowercase_items

    def items(self):
        return self._items

    def iteritems(self):
        return iter(self._items)

def pluralize(word):
    word = word.strip()
    if word == '':
        return ''
    elif word[-1].lower() == 'y':
        return '%sies' % word[:-1]
    elif word[-1].lower() == 'x':
        return '%ses' % word
    else:
        return '%ss' % word

def once():
    yield True
    while True:
        yield False

def entity_encode(string):
    '''
    converts non-ascii characters to xml character references

    '''

    if string is None:
        return None
    else:
        return string.encode('ascii', 'xmlcharrefreplace')

def uniq(iterable):
    return (x[0] for x in groupby(sorted(iterable)))

def cached(function):
    '''
    caches a function's return value. Only works for instance methods.

    '''

    cache_name = '___cache__%s' % (function.__name__,)
    def decorate(self, *args, **kwargs):
        kwarg_list = tuple(sorted(kwargs.iteritems()))

        if not hasattr(self, cache_name):
            setattr(self, cache_name, {})
        if getattr(self, cache_name).has_key((args, kwarg_list)):
            return getattr(self, cache_name)[(args, kwarg_list)]
        else:
            value = function(self, *args, **kwargs)
            getattr(self, cache_name)[(args, kwarg_list)] = value
            return value

    decorate.__name__  = function.__name__
    decorate.__doc__   = function.__doc__
    decorate.__dict__  = function.__dict__
    decorate.__cache_name__ = cache_name
    return decorate

def chunks(lst, length):
    '''
    splits an iterable into chunks of size ``length``, that is:
    chunks(xrange(10), 3) == ([0,1,2], [3,4,5], [6,7,8], [9])

    '''

    accum = []
    count = 0
    for item in lst:
        count += 1
        accum.append(item)
        if count % length == 0:
            yield accum
            accum = []
    if accum:
        yield accum

def localdatetime():
    from datetime import datetime
    from time import localtime
    return datetime(*localtime()[:6])

def strip(arg):
    return arg.strip(u'\xa0 \t\n\r')

