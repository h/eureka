#TODO: don't cache retried errors when cookies are disabled (?)
import urllib2
import httplib
import logging
import urllib
import urlparse
import socket
from time import time, sleep
from functools import partial
from random import random
from eureka.misc import urldecode, urlencode, short_repr
from sys import stderr
from copy import copy
from itertools import tee, izip

__all__ = ('firefox_user_agent', 'default_user_agent', 'crawler', 'Crawler')

# in case we want to be firefox... don't do this
firefox_user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.0; en-US; ' \
                     'rv:1.9.0.5) Gecko/2008120122 Firefox/3.0.5'

# if anybody has concerns about the bot crawling their site, they can contact
# us at the hoffson bot email address
default_user_agent = 'Eureka'

class Crawler():
    '''
    A Web crawler that correctly handles cookies, user-agents and http
    referers.

    If a ``delay`` is specified in the constructor, the Crawler will wait that
    many seconds between http requests. If the delay is a tuple, say (5, 8),
    then the crawler will sleep a random number of seconds between
    5 and 8.

    If ``retries`` is specified in the contstructor, the Crawler will re-try
    downloading timed-out pages that many times.

    If ``silent`` is set to False in __init__, the crawler will print the url
    of the downloading page for every request.

    If ``cache`` is non-Null, page-loads with be cached with sqlite. If it is
    set to True, the default cache ``eureka.cache.cache`` will be used. To use
    a non-default cache, set the ``cache`` parameter to a eureka.cache.Cache
    object.

    If ``cookies`` is set to True, the crawler correctly sets and attaches
    cookies to its requests. If ``cookies`` is set to a CookieJar object, that
    CookieJar object is used to store and handle http cookies.

    If ``cache_control`` is not null, all fetches will automatically use 
    cache-control with a standard counter variable.

    '''

    def __init__(self, cookies=True, user_agent=default_user_agent,
            delay=0, retries=0, cache=True, silent=False, robotstxt=True,
            verbose=False, truncate=False, cache_control=False, sanitize=False):

        http_processors = []

        if robotstxt:
            import robotstxt
            http_processors.append(robotstxt.RobotsTxt())

        if cache is True: # yes, this is correct
            from eureka.cache import cache

        if cache:
            self.cache = cache
            http_processors.append(cache)
        else:
            self.cache = None

        if cache_control:
            from itertools import count
            self.cache_control = count()
        else:
            self.cache_control = None

        if cookies is not None and cookies is not False:
            if cookies is True:
                import cookielib
                self.cookies = cookielib.CookieJar()
            else:
                self.cookies = cookies
            http_processors.append(urllib2.HTTPCookieProcessor(self.cookies))
        else:
            self.cookies = None

        if not silent:
            # http requests are printed whenever a request is made
            self._http_request_printer = HTTPRequestPrinter(verbose=verbose, truncate=truncate)
            http_processors.append(self._http_request_printer)

        if delay != 0:
            if isinstance(delay, int) or isinstance(delay, float):
                delay = (delay,)
            http_processors.append(HTTPDelay(*delay))

        self.opener = urllib2.build_opener(*http_processors)

        self.user_agent = user_agent
        if user_agent:
            self.opener.addheaders = [('User-Agent', user_agent)]
        else:
            self.opener.addheaders = []

        self.sanitize = sanitize

        self.retries = retries

    def _open_http(self, method, url, values, **extra_args):
        '''
        A wrapper around Crawler.fetch(). This method has the format expected
        by lxml.html.submit_form()

        '''

        if method == 'POST':
            return self.fetch(url, data=values, **extra_args)
        else:
            return self.fetch(add_parameters_to_url(url, values),
                              **extra_args)

    # IMPORTANT: if any arguments are added to fetch(), they must be added
    # below, as well, next to the other "IMPORTANT" comment
    def fetch(self, url, data=None, headers={}, referer=True,
              cache_control=None, retries=None):
        '''
        Fetches the data at the given url. If ``data`` is ``None``, we use
        a GET request, otherwise, we use a POST request.

        ``data`` can either be a dictionary or a urlencoded string, or an
        ordered list of two-tuples [('key1','value1'),('key2','value2'), ...]

        If ``referer`` is False or empty, we don't send the http referer
        header; otherwise, we send the specified referer. If ``referer`` is
        True, we try to figure out the correct referer form the ``url``

        If a ``cache_control`` parameter is specified, only cached pages with
        the same cache_control will be used.

        If a ``retries`` integer argument is specified, page fetches will be
        retried ``retries`` times on page-load errors.

        '''

        if self.cache_control and not cache_control:
            cache_control = self.cache_control.next()

        # determine the correct referer to use
        if referer is True: # yes, this is right
            # try to determine the correct referer from the url
            if hasattr(url, 'base_url'):
                referer = url.base_url
            elif (hasattr(url, 'getparent') and
                  hasattr(url.getparent(), 'base_url')):
                referer = url.getparent().base_url
            else: # nope... can't determine referer from url
                referer = None
        if referer:
            headers['Referer'] = referer
        if retries is None:
            retries = self.retries

        # if we are passed a 'form' object in stead of a url, submit the form!
        if not isinstance(url, basestring):
            from lxml import html # don't import lxml.html until we need it!
            if isinstance(url, html.FormElement):
                # IMPORTANT: if any arguments are added to the fetch()
                # function, then they must be added here, as well!!!
                http = partial(self._open_http, headers=headers,
                               referer=referer, cache_control=cache_control,
                               retries=retries)
                return html.submit_form(url, extra_values=data, open_http=http)
            else:
                raise ValueError('Crawler.fetch expects url of type '
                                 '<basestring> or <FormElement>. Got: %s'
                                 % type(url))

        # the post-data needs to be url-encoded if it isn't a string
        if data is not None and not isinstance(data, basestring):
            data = urlencode(data, doseq=1)

        # alright, we're ready to download the page!
        request = urllib2.Request(url, data=data, headers=headers)
        if cache_control is not None:
            request.cache_control = str(cache_control)

        # download multiple times in case of url-errors...
        error = None
        for retry in xrange(retries + 1):
            try:
                result = self.opener.open(request)
                result.__enter__ = lambda: result
                result.__exit__ = lambda x,y,z: result.close()
                return result

            except urllib2.HTTPError, e:
                if 500 <= e.code < 600 :
                    # if many errors happen, retain the first one
                    print 'passing_HTTP_ERROR:' + url + ':retry:' + str(retry)
                    error = error or e
                    import time
                    import random
                    time.sleep(5 * 2**min(retry, 8) * random.random())
                else:
                    raise

            except urllib2.URLError, e:
                # check whether we should re-try fetching the page
                if e.reason.strerror not in ('Connection refused',):
                    # don't retry downloading page if a non-http error
                    # happened
                    raise e
                else:
                    # if many errors happen, retain the first one
                    print 'passing_CONNECTION_ERROR:' + url + ':retry:' + str(retry)
                    time.sleep(5 * 2**min(retry, 8) * random.random())
                    error = error or e

            except (httplib.IncompleteRead, httplib.BadStatusLine, socket.error), e:
                error = error or e
                import time
                import random
                print 'passing_NETWORK_ERROR:' + url + ':retry:' + str(retry)
                time.sleep(5 * 2**min(retry, 8) * random.random())

        # we can only get here, if an error occurred
        print 'RAISE_ERROR:::::' + url + ':RETRIED:' + str(retry)
        raise error

    def fetch_xml(self, *args, **kwargs):
        '''
        Makes a request to ``url`` and returns the result parsed as xml.

        A keyword argument ``encoding`` can be specified to override the page's
        default encoding.

        '''

        from eureka.xml import XMLParser
        from lxml import etree

        encoding = kwargs.pop('encoding', None)

        with self.fetch(*args, **kwargs) as fp:
            result = etree.parse(fp, parser=XMLParser(encoding=encoding)).getroot()
            return result

    def fetch_pdf(self, url, command=None, xml=None, extra_args=None, *args, **kwargs):
        '''
        Fetches a pdf file, and returns it as converted into xml or html. This
        requires pdftohtml to be installed; see eureka/pdf.html.

        The arguments `command`, `xml` and `extra_args` have the same meaning
        as in pdftohtml in eureka/pdf.py

        '''

        from eureka.pdf import pdftohtml

        with self.fetch(url, *args, **kwargs) as fp:
            converter_args = {}
            if xml is not None:        converter_args['xml'] = xml
            if command is not None:    converter_args['command'] = command
            if extra_args is not None: converter_args['extra_args'] = extra_args

            return pdftohtml(fp, **converter_args)

    def fetch_xhtml(self, *args, **kwargs):
        '''
        Like ``fetch_xml``, but we expect an XHTML response.

        A keyword argument ``encoding`` can be specified to override the page's
        default encoding.

        '''

        from eureka.xml import XHTMLParser
        from lxml import etree

        encoding = kwargs.pop('encoding', 'utf-8')

        with self.fetch(*args, **kwargs) as fp:
            result = etree.parse(fp, parser=XHTMLParser(encoding=encoding)).getroot()
            result.make_links_absolute(fp.geturl(), handle_failures='ignore')
            return result

    def fetch_html(self, *args, **kwargs):
        '''
        Like ``fetch_xml``, but we expect an HTML response.

        A keyword argument ``encoding`` can be specified to override the page's
        default encoding.

        '''

        from eureka.xml import HTMLParser
        from lxml import etree

        encoding = kwargs.pop('encoding', None)

        error = None
        for retry in xrange(self.retries+1):
          try:
            with self.fetch(*args, **kwargs) as fp:
                if self.sanitize:
                    from StringIO import StringIO
                    raw = fp.read()
                    processed = raw.decode('ascii', 'ignore').encode(
                            'utf-8', 'ignore')
                    result = etree.parse(
                            StringIO(processed), parser=HTMLParser(encoding=encoding)
                            ).getroot()
                else:
                    result = etree.parse(
                        fp, parser=HTMLParser(encoding=encoding)).getroot()

                result.make_links_absolute(fp.geturl(), handle_failures='ignore')
                return result
          except httplib.IncompleteRead, e:
            error = error or e
            import time
            import random
            print 'passing_INCOMPLETE_READ.retry:' + str(retry)
            time.sleep(5 * 2**min(retry, 8) * random.random())

        print 'raising_INCOMPLETE_READ:RETRIED:' + str(retry)
        raise error

    def fetch_broken_html(self, *args, **kwargs):
        '''
        like ``fetch_html`` with even more relaxed parsing by using
        ``BeautifulSoup`` as our parser

        '''

        from lxml.html import soupparser
        from eureka.xml import HTMLParser

        with self.fetch(*args, **kwargs) as fp:
            result = soupparser.parse(fp,
                     makeelement=HTMLParser().makeelement).getroot()
            result.make_links_absolute(fp.geturl(), handle_failures='ignore')
            return result

class HTTPRequestPrinter(urllib2.BaseHandler):
    '''
    A URL-handler that prints HTTP requests as they are performed. There are
    two possible formats:

     - verbose: prints the full HTTP request that will be sent to the server
     - non-verbose: prints an abbreviation of the request method, as well as
       the requested URL

    '''

    handler_order = 999 # run this at the very end

    def __init__(self, verbose=False, truncate=False):
        self.verbose = verbose
        self.truncate = truncate

    def http_request(self, request):
        logging.debug(self.request_description(request))
        return request

    https_request = http_request

    def http_error_default(self, req, fp, code, message, headers):
        stderr.write('.')
        stderr.flush()
        logging.debug('.. failed')

    def http_response(self, request, response):

        # write first dot to stderr, so it is printed when logging is disabled
        stderr.write('.')
        stderr.flush()

        if hasattr(response, 'is_from_cache'):
            logging.debug('.. cached')
        elif 300 <= response.code < 400:
            logging.debug('.. redirect')
        else:
            logging.debug('.. done')
        return response

    https_response = http_response

    def request_description(self, request):
        if self.verbose:
            return HTTPRequestPrinter.verbose_request_description(request,
                    truncate=self.truncate)
        else:
            return HTTPRequestPrinter.basic_request_description(request)

    @staticmethod
    def basic_request_description(request, truncate=False):
        '''
        Returns an abbreviated description of the request, including the
        request method (GET/POST), as well as an abbreviated url.

        truncate isn't used, but is taken as an input for consistency with
        verbose_request_description

        '''

        return '%s: %s' % ((request.data is None) and 'GET' or 'POST',
                           short_repr(request.get_full_url(), 48))

    @staticmethod
    def verbose_request_description(request, truncate=False):
        '''
        Returns the entire HTTP request the way it is sent to the server.

        '''

        result = []
        _, _, path, query_string, _ = \
                urlparse.urlsplit(request.get_full_url())
        path = path or '/' # the empty path is actually a "/" path in HTTP!
        url = urlparse.urlunsplit(('', '', path, query_string, ''))
        result.append('%s %s HTTP/1.1' % (request.get_method(), url))
        for header_name, header_value in request.header_items():
            if truncate:
                result.append('%s: %s' % (header_name[:1000], 
                                          header_value[:1000]))
            else:
                result.append('%s: %s' % (header_name, header_value))

        # print post data, if the request has any
        if request.data is not None:
            result.append('')
            postdata = request.data.split('&')
            if truncate:
                postdata = [x[:1000] for x in postdata]
            result.append('\n'.join(postdata))

        result.append('') # end the output with a newline
        return '\n'.join(result)

class HTTPDelay(urllib2.BaseHandler):
    ''' This handler causes a delay before each http request. '''

    # run this after the cache, so we don't cause delays when a page is cached
    handler_order = 301

    def __init__(self, min_delay, max_delay=None):
        '''
        The delay is a random number between min_delay and max_delay. If
        max_delay is not set, it will be set to be equal to the min_delay.

        '''

        self.last_request_time = 0
        self.min_delay = min_delay

        if max_delay is None:
            self.max_delay = min_delay
        else:
            if max_delay < min_delay:
                raise ValueError('max_delay must be larger than min_delay')
            self.max_delay = max_delay

    def http_open(self, request):
        '''
        If the last request was less than ``self.delay`` seconds ago, we sleep
        for a while.

        '''

        # get a random number, if delay is specified as a tuple
        if self.min_delay != self.max_delay:
            delay = self.min_delay \
                  + random() * (self.max_delay-self.min_delay)
        else:
            delay = self.delay

        # figure out how long we need to sleep
        cur_time = time()
        sleep_time = self.last_request_time + delay - cur_time

        if sleep_time <= 0:
            self.last_request_time = cur_time
        else:
            sleep(sleep_time)
            self.last_request_time = time()

    https_open = http_open

def add_parameters_to_url(url, values):
    '''
    adds ``values`` as url-encoded GET parameters to ``url``. ``values`` is
    specified as a dictionary.

    '''

    scheme, netloc, path, params, query, fragment = urlparse.urlparse(url)
    query_list = urldecode(query)
    if len(query_list) < 2:
        print(url)
        print(query_list)

    # remove these keys from the GET parameters, as we are specifying new
    # values for them...
    delete_keys = set(dict(values))
    new_query = []

    for key, value in query_list:
        if key not in delete_keys:
            new_query.append((key, value))
    for key, value in values:
        new_query.append((key, value))

    query = urlencode(new_query, doseq=1)
    return urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

crawler = Crawler()
