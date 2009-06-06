from __future__ import with_statement

import re
from collections import defaultdict

from eureka import EurekaException
from eureka.misc import once
from lxml import etree
from lxml import html
import sys

class EurekaXPathError(etree.XPathError, EurekaException):
    def __init__(self, xml, message):
        super(EurekaXPathError, self).__init__(message)
        self.xml = xml

def _input_or_quit():
    '''
    Waits for user input.

    '''

    if not sys.stdin.isatty():
        print "Can't get keyboard input. Quitting..."
        sys.exit(2)
    else:
        response = raw_input('Continue running script? [Y]/n: ')
        if response.lower().strip() == 'n':
            sys.exit(2)

class EurekaElement(etree.ElementBase):
    '''
    This is a mixin for the other element classes, that allows us to use custom
    ``select``, ``__call__`` and ``xpath``, etc functions. Otherwise, this is
    identical to etree.ElementBase.

    '''

    def select(self, _path, join_function=None, *args, **kwargs):
        '''
        essentially equivalent to ``xml.xpath(...)[0]``. A ``default`` keyword
        argument may be specified, which is returned when no results are found.

        if a ``join_function`` is specified and there are multiple matching
        results, the results will be passed to the join_function, in stead of
        raising an error.

        '''

        result = self.xpath(_path, *args, **kwargs)
        if isinstance(result, list):
            if len(result) == 1:
                return result[0]
            elif len(result) == 0 and kwargs.has_key('default'):
                return kwargs['default']
            elif len(result) > 1 and join_function is not None:
                return join_function(result)
            else:
                raise EurekaXPathError(self,
                    '\n\n  Got %s results for XPath expression: "%s"\n'
                    '  Expected one result. XPath was run on xml tag "<%s>"\n'
                    % (len(result), _path, self.tag))
        else:
            return result

    def __call__(self, _path, join=None, *args, **kwargs):
        '''
        synonym for self.select(...).text.
        
        If a ``join`` string is specified and there are multiple results, they
        will be concatenated with the join string, in stead of returning an
        error.

        '''

        def convert_to_text(output):
            ''' converts potential XML Elements to text '''
            if isinstance(output, etree.ElementBase):
                return output.text or ''
            else:
                return output

        # pass a join_function to select() that concats results with ``join``
        if join is not None:
            join_function = lambda x: join.join(convert_to_text(y) for y in x)
        else:
            join_function = None

        result = self.select(_path, join_function=join_function, *args, **kwargs)
        return convert_to_text(result)

    def xpath(self, _path, namespaces=None, *args, **kwargs):
        '''
        gives us access to our pre-defined namespaces
        e=http://schedulizer.com/eureka, eureka=http://schedulizer.com/eureka
        and re=http://exslt.org/regular-expressions

        '''

        if namespaces is None:
            namespaces = {}
        #namespaces['fn'] = 'http://www.w3.org/2005/02/xpath-functions'
        namespaces['e'] = 'http://schedulizer.com/eureka'
        namespaces['eureka'] = 'http://schedulizer.com/eureka'
        namespaces['re'] = 'http://exslt.org/regular-expressions'
        try:
            return etree.ElementBase.xpath(self, _path, namespaces=namespaces, *args, **kwargs)
        except etree.XPathError, e:
            raise EurekaXPathError(self,
                '\n\n  Error for xpath expression: "%s".\n'
                '  Error was: %s\n' % (_path, e.message))

    def tostring(self, pretty_print=False, encoding=None, method="xml"):
        '''
        see lxml.etree.tostring

        '''

        return etree.tostring(self, pretty_print=pretty_print, method=method,
                              encoding=encoding)

    def write(self, filename, wait=False, *args, **kwargs):
        '''
        Saves the xml node to a file.

        If ``wait`` is set to ``True``, we wait for user input before
        continuing to run the program. This is useful for using a DOM inspector
        in the xml node.

        '''

        write_doc = etree.ElementTree(element=self).write
        write_doc(filename, *args, **kwargs)
        if not isinstance(filename, basestring):
            filename.flush()
        if wait:
            print
            print 'Wrote xml to "%s".' % filename
            _input_or_quit()

    def open_in_browser(self, browser=None, new=1, wait=True):
        '''
        openes this xml node in a browser. If ``browser`` is specified as a string (eg.
        'firefox'/'konqueror'/etc), we try to use that browser. Otherwise, we
        use the system's default browser.

        If new is ``1``, we try to open the browser in a new window. If new is
        ``2``, we try to open the browser in a new tab.

        If ``wait`` is set to ``True``, we wait for user input before
        continuing to run the program

        '''

        import sys, os, webbrowser
        from os.path import abspath
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile('w+b', suffix='.html', prefix='eureka') as fp:
            self.write(fp, method='html', pretty_print=True, wait=False)
            url = 'file://' + abspath(fp.name).replace(os.path.sep, '/')
            print 'Opening webbrowser: ',
            try:
                browser = webbrowser.get(browser)
            except Exception:
                browser = webbrowser.GenericBrowser(browser)
            print browser.open(url, new=new)

            # wait for user input before continuing to run the script. Otherwise,
            # we may end up opening a lot of windows at once!
            if wait:
                _input_or_quit()

def normalize_spaces(string):
    return re.sub('\s\s*', ' ', string).strip()

class EurekaOptionElement(EurekaElement):
    '''
    If an option element doesn't specify a "value" attribute, we set the
    option's value to self.text
    '''

    @property
    def value(self):
        if 'value' in self.attrib:
            return self.get('value')
        else:
            return normalize_spaces(self.text or '')

class EurekaMultipleSelectOptions(html.MultipleSelectOptions):
    def __iter__(self):
        for option in self.options:
            if 'selected' in option.attrib:
                yield option.value

class EurekaSelectElement(EurekaElement):
    """
    Uses a custom MultipleSelectElement the doesn't have lxml's bugs, and
    handle elements without "value" attributes correctly.

    """

    @property
    def options(self):
        ''' returns the option elements of this select element '''

        return iter(html._options_xpath(self))

    @property
    def options_dict(self):
        '''
        a mapping that maps values to the corresponding option element with
        that value

        '''

        return dict((option.value, option) for option in self.options)

    def _value__get(self):
        """
        Get/set the value of this select (the selected option).

        If this is a multi-select, this is a set-like object that
        represents all the selected options.

        If no option is selected, we default to using the first option.
        ... not sure if this is optimal.

        """

        if self.multiple:
            return EurekaMultipleSelectOptions(self)
        else:
            first_option = None
            for el in self.options:
                if 'selected' in el.attrib:
                    return el.value
                if first_option is None:
                    first_option = el
            return first_option

    def _value__set(self, value):
        if self.multiple:
            if isinstance(value, basestring):
                value = (value,)
            self.value.clear()
            self.value.update(value)
            return
        if value is not None:
            for el in self.options:
                if el.value == value:
                    checked_option = el
                    break
            else:
                raise ValueError(
                    "There is no option with the value of %r" % value)
        for el in self.options:
            if 'selected' in el.attrib:
                del el.attrib['selected']
        if value is not None:
            checked_option.set('selected', '')

    def _value__del(self):
        if self.multiple:
            self.value.clear()
        else:
            self.value = None

    value = property(_value__get, _value__set, _value__del, doc=_value__get.__doc__)

# {{{ functions on html input elements needed for ``EurekaFormElement``
def _get_input_value(input_element):
    '''
    Gets the value of an html input element. Handles MultipleSelectOptions
    fields correctly.

    '''

    if input_element.multiple:
        # assume only one option is selected
        return iter(input_element.value).next()
    else:
        return input_element.value
# }}} functions on html input elements needed for ``EurekaFormElement``

class EurekaFormElement(EurekaElement):
    def _clean_iterate_fields(self, args):
        ''' prepare the given fields into a standard format '''

        for field in args:
            if isinstance(field, tuple) or isinstance(field, list):
                if len(field) == 2:
                    field, regex1 = field
                    regex2 = '[^ ]'
                elif len(field) == 3:
                    field, regex1, regex2 = field
                else:
                    raise ValueError('iterate_fields() was given an argument '
                                     'of length greater than 3!')
            else:
                regex1 = regex2 = '[^ ]'

            if isinstance(field, basestring):
                if field not in self.inputs.keys():
                    raise KeyError("The field %s passed to "
                        "form.iterate_fields doesn't exist in the form. The "
                        "form has input elements: %s" % (field, self.fields))
                else:
                    field = self.inputs[field]
            yield (field, regex1, regex2)

    def iterate_fields(self, *args):
        '''
        Just like ``iterate_options``, but returns each option's "value"
        attribute.

        '''

        for option in self.iterate_options(*args):
            yield option.value

    def iterate_options(self, *args):
        '''
        Iterates through all possible values of the input elements in ``args``.

        At each iteration step, we yield the current values of the input
        elements.

        Eg. to loop over all possible option elements of the "term" and "year"
        fields in a form, you could write:

        > form = some_html.forms[0]
        > for year, term in form.iterate_options('year', 'term'):
        >     html = crawler.fetch_html(form)
        >     ...

        each of the ``args`` elements can either be a string, or an html input
        element. It could also be a tuple, in which case the first tuple
        entry is the field to be crawled. The second entry of the tuple is the
        regex that the "value" attribute of an OPTION element must match to be
        included in the result set. The third entry is the regex that the
        content text of an OPTION element must match to be included.

        '''

        if not len(args):
            return

        # prepare arguments into standardized format
        args = tuple(self._clean_iterate_fields(args))

        # iterate through all options
        stack = []
        field, _, _ = args[0]
        stack.append((None, iter(field.options)))
        while stack:
            field, regex1, regex2 = args[len(stack)-1]
            previous_option, options = stack.pop()

            try:
                cur_option = options.next()
                stack.append((cur_option, options))
            except StopIteration:
                continue

            if regex1 and not re.search(regex1, cur_option.value) or \
               regex2 and not re.search(regex2, cur_option.text):
                continue # skip to the next iteration if regexes don't match

            field.value = cur_option.value

            if len(stack) < len(args):
                field, _, _ = args[len(stack)]
                stack.append((None, iter(field.options)))
            else:
                result = tuple(option for option, options in stack)
                if len(result) == 1:
                    # no need to use tuples, if there is only one result
                    yield result[0]
                else:
                    yield result

xml_parser_lookup = etree.ElementDefaultClassLookup(element=EurekaElement)
html_parser_lookup = html.HtmlElementClassLookup(
        mixins=(('option', EurekaOptionElement), ('form', EurekaFormElement),
                ('select', EurekaSelectElement), ('*', EurekaElement),))

xml_parser = etree.XMLParser()
xml_parser.setElementClassLookup(xml_parser_lookup)

html_parser = etree.HTMLParser()
html_parser.setElementClassLookup(html_parser_lookup)

xhtml_parser = etree.XMLParser()
xhtml_parser.setElementClassLookup(html_parser_lookup)

XML = lambda string: etree.fromstring(string, parser=xml_parser)
HTML = lambda string: html.fromstring(string,
                                      parser=html_parser)
XHTML = lambda string: html.fromstring(string,
                                       parser=xhtml_parser)

# {{{ lxml Hack
# this is a hack to get field dictionaries to display nicely
del html.FieldsDict.__repr__
def _InputGetter__repr__(self):
    return repr(dict(((key, self[key]) for key in self.keys())))
html.InputGetter.__repr__ = _InputGetter__repr__

# This is a hack to get default elements working with the html_parser_lookup.
# We should probably email the lxml guys so they can fix this.
EurekaHtmlElement = type(html.HtmlElement.__name__, (EurekaElement, html.HtmlElement), {})
class _ElementClasses(object):
    def __init__(self, lookup):
        self.lookup = lookup
    def get(self, arg, ignore):
        return self.lookup.get(arg, EurekaHtmlElement)
html_parser_lookup._element_classes = _ElementClasses(html_parser_lookup._element_classes)
# }}} lxml Hack


# XPath/XSLT namespaces
class XPathNamespace(object):
    def __init__(self, prefix, name):

        self.namespace = etree.FunctionNamespace(name)
        self.namespace.prefix = prefix

    def define(self, value, name=None):
        '''
        Adds a function to this namespace. Intended to be used like:

        namespace = XPathNamespace()
        @namespace.define
        def myfunction(): pass

        '''

        if name is None:
            self.namespace[value.func_name] = value
        else:
            self.namespace[name]  = value
        return value

    def define_simple(self, value, name=None):
        '''
        similar to ``define``, but we don't need to worry about the
        ``context`` argument and element lists, etc. The defined function
        will be applied to the text attribut of elements, not elements
        themselves.

        Example usage:
        >>> ns = XPathNamespace('http://schedulizer.com/eureka')
        >>> @ns.define_simple
        >>> def strip(arg):
        >>>     return arg.strip()

        '''

        def wrapper(context, arg, *args):
            if isinstance(arg, list):
                return [wrapper(context, x, *args) for x in arg]
            elif isinstance(arg, etree.ElementBase):
                arg = arg.__copy__()
                arg.text = str(value(arg.text, *args))
                return arg
            else:
                result = value(arg, *args)
                if isinstance(result, basestring):
                    # Hack around but in lxml. Sigh. Fix this once lxml
                    # gets fixed.
                    tmp = xml_parser.makeelement('text')
                    tmp.text = result
                    result = tmp

                    #result = etree._ElementStringResult(result)
                    if context is not None:
                        result._parent = context.context_node
                    #result.is_tail = False
                    #result.is_attribute = False
                    #result.is_text = True
                return result

        if name is None:
            self.namespace[value.func_name] = wrapper
        else:
            self.namespace[name]  = wrapper
        return value

# {{{ some useful functions for xpath and xslt
eureka_namespace = XPathNamespace('e', 'http://schedulizer.com/eureka')
@eureka_namespace.define_simple
def strip(arg):
    return arg.strip()
# }}} some useful functions for xpath and xslt
