import urllib2
import urllib
import urlparse
from time import time, sleep
from functools import partial
from random import random
from eureka.misc import urldecode, short_repr
from sys import stdout

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
                 delay=0, retries=0, cache=True, silent=False, robotstxt=True):

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

    def fetch(self, url, data=None, headers={}, referer=True,
              cache_control=None):
        '''
        Fetches the data at the given url. If ``data`` is ``None``, we use
        a GET request, otherwise, we use a POST request.

        ``data`` can either be a dictionary or a urlencoded string

        If ``referer`` is False or empty, we don't send the http referer
        header; otherwise, we send the specified referer. If ``referer`` is
        True, we try to figure out the correct referer form the ``url``

        If a ``cache_control`` parameter is specified, only cached pages with
        the same cache_control will be used.

        '''

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

        # if we are passed a 'form' object in stead of a url, submit the form!
        if not isinstance(url, basestring):
            from lxml import html # don't import lxml.html until we need it!
            if isinstance(url, html.FormElement):
                http = partial(self._open_http, headers=headers,
                               cache_control=cache_control)
                return html.submit_form(url, extra_values=data, open_http=http)

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
        if not self.silent:
            print '%s: %s ...' % ((data is None) and 'GET' or 'POST',
                                 short_repr(url, 48)),
            # we need to flush stdout, since we didn't print a newline
            stdout.flush()

        # download multiple times in case of url-errors...
        error = None
        for retry in xrange(self.retries + 1):

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
                        print 'cached'
                else:
                    if not self.silent:
                        print 'done'

                return result
            except urllib2.HTTPError, e:
                # if many errors happen, retain the first one
                error = error or e
                if not self.silent:
                    if retry < self.retries:
                        print 'retrying ...',
                    else:
                        print 'failed'
            except urllib2.URLError, e:
                if not self.silent:
                    print 'failed'
                raise e # don't retry downloading page if URLError occurred...

        # we can only get here, if an error occurred
        print '------------------------'
        print '  HTTP code %s for "%s"' % (error.code, url)
        print '  With post data "%s"' % data
        raise error

    def fetch_xml(self, *args, **kwargs):
        '''
        Makes a request to ``url`` and returns the result parsed as xml.

        '''

        from eureka.xml import xml_parser
        from lxml import etree

        return etree.parse(self.fetch(*args, **kwargs),
                           parser=xml_parser).getroot()

    def fetch_xhtml(self, *args, **kwargs):
        '''
        Like ``fetch_xml``, but we expect an XHTML response.

        '''

        from eureka.xml import xhtml_parser
        from lxml import etree

        data = self.fetch(*args, **kwargs)
        result = etree.parse(data, parser=xhtml_parser).getroot()
        result.make_links_absolute(data.geturl())
        return result

    def fetch_html(self, *args, **kwargs):
        '''
        Like ``fetch_xml``, but we expect an HTML response.

        '''

        from eureka.xml import html_parser
        from lxml import etree

        data = self.fetch(*args, **kwargs)
        result = etree.parse(data, parser=html_parser).getroot()
        result.make_links_absolute(data.geturl())
        return result

    def fetch_broken_html(self, *args, **kwargs):
        '''
        like ``fetch_html`` with even more relaxed parsing by using
        ``BeautifulSoup`` as our parser

        '''

        from lxml.html import soupparser
        from eureka.xml import html_parser

        data = self.fetch(*args, **kwargs)
        result = soupparser.parse(data,
                     makeelement=html_parser.makeelement).getroot()
        result.make_links_absolute(data.geturl())
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
        if key not in delete_keys:
            new_query.append((key, value))

    query = urllib.urlencode(new_query)
    return urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

crawler = Crawler()
default_crawler = crawler
