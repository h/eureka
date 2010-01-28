from __future__ import with_statement

import urllib2
import urllib
import urlparse
from time import time, sleep
from functools import partial
from random import random
from eureka.misc import urldecode, short_repr
from sys import stderr
from eureka.pdf import pdftohtml
from copy import copy
import logging

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

    If ``cache`` is set to True, page-loads with be cached with sqlite. See
    ``eureka.cache``.

    '''

    def __init__(self, cookies=True, user_agent=default_user_agent,
            delay=0, retries=0, cache=True, silent=False, robotstxt=True,
            verbose=False):

        # if robotstxt is True, we make sure that no page we fetch is
        # disallowed in the robots.txt of the site
        if robotstxt:
            from eureka.robotstxt import RobotsTxt
            self.can_fetch = RobotsTxt(self.fetch).can_fetch
        else:
            self.can_fetch = lambda x,y,silent=False: True

        http_processors = ()

        if cache is True: # yes, this is correct
            from eureka.cache import cache

        if cache:
            self.cache = cache
            http_processors += (cache,)
        else:
            self.cache = None

        if cookies:
            import cookielib
            self.cookies = cookielib.CookieJar()
            http_processors += (urllib2.HTTPCookieProcessor(self.cookies),)
        else:
            self.cookies = None

        self.opener = urllib2.build_opener(*http_processors)

        self.user_agent = user_agent
        if user_agent:
            self.opener.addheaders = [('User-Agent', user_agent)]
        else:
            self.opener.addheaders = []

        self.silent = silent
        self.retries = retries
        self.delay = delay
        self.verbose=verbose
        self.last_request_time = 0

    def _wait_for_delay(self):
        '''
        if the last request was less than ``self.delay`` seconds ago, we sleep
        for a while.

        '''

        # get a random number, if delay is specified as a tuple
        if isinstance(self.delay, tuple) or isinstance(self.delay, list):
            start, end = self.delay
            delay = start + random() * (end-start)
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

    def verbose_request_description(self, request):
        '''
        More descriptive version of request_string: returns the request string
        containing the entire URL, HTTP Headers and Postdata.

        '''

        req = copy(request) # This request object is used for formatting, only

        # copied from urllib2.py
        protocol = req.get_type()
        meth_name = protocol + '_request'
        for processor in self.opener.process_request.get(protocol, []):
            meth = getattr(processor, meth_name)
            req = meth(req)

        # now create the request description string...
        result = []
        _, _, path, query_string, _ = urlparse.urlsplit(req.get_full_url())
        url = urlparse.urlunsplit(('', '', path, query_string, ''))
        result.append('%s %s' % (req.get_method(), url))
        for header_name, header_value in req.header_items():
            result.append('%s: %s' % (header_name, header_value))
        # print query string parameters
        if query_string:
            result.append('QUERY STRING %s' %
                          self._postdata_description(query_string))
        # print POST data
        if req.data is not None:
            result.append('POST DATA %s' %
                          self._postdata_description(req.data))

        return '\n'.join(result)

    def _postdata_description(self, urlencoded):
        '''
        Returns a string describing the urlencoded data.

        Eg. "a=b&c=d" would turn into:
        "{
           a: b
           c: d
         }"

        The string "" is turned into "{}"

        '''

        result = []
        if not urlencoded:
            result.append('{}')
        else:
            result.append('{')
            for segment in urlencoded.split('&'):
                name, _, value = segment.partition('=')
                name  = urllib.unquote_plus(name)
                value = urllib.unquote_plus(value)
                result.append('  %s: %s' % (name, value))
            result.append('}')
        return '\n'.join(result)

    def request_description(self, request):
        '''
        Returns an abbreviated description of the request, including the
        request method (GET/POST), as well as an abbreviated url.

        '''

        return '%s: %s' % ((request.data is None) and 'GET' or 'POST',
                           short_repr(request.get_full_url(), 48))

    def fetch(self, url, data=None, headers={}, referer=True,
              cache_control=None, retries=None, verbose=None):
        '''
        Fetches the data at the given url. If ``data`` is ``None``, we use
        a GET request, otherwise, we use a POST request.

        ``data`` can either be a dictionary or a urlencoded string

        If ``referer`` is False or empty, we don't send the http referer
        header; otherwise, we send the specified referer. If ``referer`` is
        True, we try to figure out the correct referer form the ``url``

        If a ``cache_control`` parameter is specified, only cached pages with
        the same cache_control will be used.

        If a ``retries`` integer argument is specified, page fetches will be
        retried ``retries`` times on page-load errors.

        '''

        if verbose is None:
            verbose = self.verbose

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
                http = partial(self._open_http, headers=headers,
                               cache_control=cache_control, retries=retries,
                               verbose=verbose)
                return html.submit_form(url, extra_values=data, open_http=http)
            else:
                raise ValueError('Crawler.fetch expects url of type '
                                 '<basestring> or <FormElement>. Got: %s'
                                 % type(url))

        # check robots.txt to make sure the page isn't disallowed!
        if not self.can_fetch(url, self.user_agent, silent=self.silent):
            from robotstxt import RobotDisallow
            raise RobotDisallow('Error: URL is disallowed in robots.txt: %s'
                                % short_repr(url, 80))

        # the post-data needs to be url-encoded if it isn't a string
        if data is not None and not isinstance(data, basestring):
            data = urllib.urlencode(data)

        # alright, we're ready to download the page!
        request = urllib2.Request(url, data=data, headers=headers)
        if cache_control is not None:
            request.cache_control = str(cache_control)

        # give a status message to the user that we're currently
        # downloading a page
        if verbose:
            request_description = self.verbose_request_description(request)
        else:
            request_description = self.request_description(request)

        if not self.silent:
            logging.info(request_description)
            stderr.write('.')
            stderr.flush()

        # download multiple times in case of url-errors...
        error = None
        for retry in xrange(retries + 1):

            # let the cache call _wait_for_delay on cache miss
            request.wait_for_delay = self._wait_for_delay
            if not self.cache:
                # if we don't have a chache, we need to call
                # wait_for_delay ourselves
                self._wait_for_delay()

            try:
                result = self.opener.open(request)

                # let the user know that we're done downloading
                if hasattr(result, 'is_from_cache'):
                    if not self.silent:
                        logging.info('.. cached')
                else:
                    if not self.silent:
                        logging.info('.. done')

                result.__enter__ = lambda: result
                result.__exit__ = lambda x,y,z: result.close()
                return result
            except urllib2.HTTPError, e:
                # if many errors happen, retain the first one
                error = error or e
                if not self.silent:
                    if retry < retries:
                        logging.info('.. retrying')
                    else:
                        logging.info('.. failed')
            except urllib2.URLError, e:
                if not self.silent:
                    logging.info('.. failed')
                raise e # don't retry downloading page if URLError occurred...

        # we can only get here, if an error occurred
        if not self.silent:
            logging.info('------------------------')
            logging.info('  HTTP code %s for "%s"' % (error.code, url))
            logging.info('  With post data "%s"' % data)
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

        encoding = kwargs.pop('encoding', None)

        with self.fetch(*args, **kwargs) as fp:
            result = etree.parse(fp, parser=XHTMLParser(encoding=encoding)).getroot()
            result.make_links_absolute(fp.geturl())
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

        with self.fetch(*args, **kwargs) as fp:
            result = etree.parse(fp, parser=HTMLParser(encoding=encoding)).getroot()
            result.make_links_absolute(fp.geturl())
            return result

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
            result.make_links_absolute(fp.geturl())
            return result

def add_parameters_to_url(url, values):
    '''
    adds ``values`` as url-encoded GET parameters to ``url``. ``values`` is
    specified as a dictionary.

    '''

    scheme, netloc, path, params, query, fragment = urlparse.urlparse(url)
    query_list = urldecode(query)

    # remove these keys from the GET parameters, as we are specifying new
    # values for them...
    delete_keys = set(dict(values))
    new_query = []
    for key, value in query_list:
        if key not in delete_keys:
            new_query.append((key, value))
    for key, value in values:
        new_query.append((key, value))

    query = urllib.urlencode(new_query)
    return urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

crawler = Crawler()
