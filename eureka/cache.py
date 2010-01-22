'''
A SQLite Cache for downloaded websites. This could make a lot of development
a lot faster.

'''

import urllib
import urllib2
import httplib
from StringIO import StringIO
from sqlite3 import Binary

from eureka import EurekaException
from eureka.misc import urldecode

class Cache(urllib2.BaseHandler):
    '''
    A database cache to store cached websites. If sqlite isn't installed, this
    does nothing.

    '''

    # call this handler after the cookie processor is done!
    handler_order = 300
    urllib2.HTTPCookieProcessor.handler_order = 200

    def __init__(self, database='web-cache.sqlite'):
        self._connection = None
        self.database = database

    def connection(self):
        if self._connection:
            return self._connection
        else:
            from eureka.database import connect
            self._connection = connect(self.database)

            if self._connection:
                self._create_tables()

            return self._connection
    connection = property(connection)

    def _create_tables(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                date DATETIME NOT NULL,

                url VARCHAR(1024) NOT NULL,
                postdata BLOB,
                headers TEXT NOT NULL,
                cache_control VARCHAR(128) NOT NULL,

                response_url VARCHAR(1024) NOT NULL,
                response_code INTEGER NOT NULL,
                response_message VARCHAR(64) NOT NULL,
                response_data BLOB NOT NULL
            )
            ''')
            cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS
                cache_index
            ON  cache (url, postdata, headers, cache_control);
            ''')
        finally:
            cursor.close()

    def clear(self, like=None):
        '''
        Clears cache-entries from the database. If ``like`` is specified, we
        will only clear cache-entries matching the SQL LIKE expression.

        Eg. cache.clear('http://%google.com/%')

        '''

        cursor = self.connection.cursor()

        if like:
            cursor.execute('DELETE FROM cache WHERE url LIKE ?', [like])
        else:
            cursor.execute('DELETE FROM cache')

        self.connection.commit()
        cursor.close()

    def _fetch(self, url, postdata, headers, cache_control):
        ''' helper method for Cache.fetch() '''

        cursor = self.connection.cursor()
        if postdata is None:
            ifnull_postdata = ''
        else:
            ifnull_postdata = postdata

        cursor.execute('''
        SELECT
            response_url, response_code, response_message,
            response_data
        FROM
            cache
        WHERE
    	    url = ? and postdata IS NULL = ? and
            CAST(IFNULL(postdata, '') AS BLOB) = ? and
            headers = ? and cache_control = ?
	''', (url, postdata is None, Binary(ifnull_postdata), headers,
              cache_control))

        result = cursor.fetchall()
        cursor.close()
        return result

    def http_open(self, request):
        '''
        If the request exists in our cache, this fetches the requested page
        from the cache.

        '''

        # if we don't support sqlite, or if the cache misses, return None
        response = None

        if self.connection:
            url = request.get_full_url()
            postdata = request.get_data()
            cache_control = getattr(request, 'cache_control', '') or ''
            headers = _serialize_headers(request.header_items())

            results = self._fetch(url, postdata, headers, cache_control)
            if len(results) > 1:
                raise EurekaException('Found multiple cache entries with '
                                      'identical http requests')
            elif len(results) == 1:
                url, code, msg, data = results[0]
                response = _make_response(url, code, msg, data)

        # if the cache missed, we need to delay before the next http request
        # XXX: this is a hack; this should go in eureka/crawler.py
        if response is None and hasattr(request, 'wait_for_delay'):
            request.wait_for_delay()

        return response

    def http_response(self, request, response):
        '''
        Stores the given response in the database, if we support sqlite.

        '''

        if self.connection:
            # Don't do anything if the response is from the cache!
            if hasattr(response, 'is_from_cache'):
                return response

            url = request.get_full_url()
            postdata = request.get_data()
            if postdata is None:
                binary_postdata = None
            else:
                binary_postdata = Binary(postdata)
            headers = _serialize_headers(request.header_items())
            cache_control = getattr(request, 'cache_control', '') or ''

            response_url = response.geturl()
            response_code = response.code
            response_message = response.msg
            response_headers = response.info()
            response_data = response.read()
            response.close()

            text = '%s\r\n%s' % \
                    (''.join(response_headers.headers), response_data)

            cursor = self.connection.cursor()
            cursor.execute('''
            INSERT INTO
                cache
                (date, url, postdata, headers, cache_control, response_url,
                 response_code, response_message, response_data)
            VALUES
                (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)
	    ''', (url, binary_postdata, headers, cache_control, response_url,
                  response_code, response_message, Binary(text)))
            self.connection.commit()
            cursor.close()

            # we read all of the response's data, so we need to create a new
            # response object with that data.
            return _make_response(response_url, response_code,
                                  response_message, text, False)
        else:
            return response

    https_open = http_open
    https_response = http_response

def _make_response(url, code, msg, data, is_from_cache=True):
    '''
    Creates a file-like response object out of the url, code, message, headers
    and data of a request. If the code is not in the 200s, return an HTTPError.

    '''

    headers = httplib.HTTPMessage(StringIO(data))

    if code >= 200 and code < 300:
        # if the request succeeded return a file-like object...
        response = urllib2.addinfourl(headers.fp, headers, url)
        response.code = code
        response.msg = msg
    else:
        # if the request failed, return an HTTPError...
        response = urllib2.HTTPError(url, code, msg, headers, headers.fp)

    # mark the result as a cached result
    if is_from_cache:
        response.is_from_cache = True
    return response

def _serialize_headers(header_items):
    lower_headers = ((k.lower(), v) for k, v in header_items)

    # ignore case for the header type
    return urllib.urlencode(sorted(lower_headers))

cache = Cache()
