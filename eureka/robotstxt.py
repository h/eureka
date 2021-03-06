import logging
import urllib2
from urlparse import urlsplit
from robotparser import RobotFileParser

class RobotDisallow(Exception):
    pass

class RobotsTxt(urllib2.BaseHandler):
    handler_order = 99 # run this before the default handlers

    def __init__(self):
        # A global cache of all robots files we've downloaded so far.
        # It is a dictionary from url to robotparser.RobotFileParser
        self.robot_files = {}

    @staticmethod
    def make_robot_url(url):
        ''' given a url determines where its associated robots.txt file is '''

        protocol, host, _, _, _ = urlsplit(url)
        return '%s://%s/robots.txt' % (protocol, host)

    @staticmethod
    def is_robot_url(url):
        ''' determinest whether the url refers to a robots.txt file '''

        return url.endswith('robots.txt')

    def http_request(self, request):
        user_agent = request.get_header('User-agent')
        url = request.get_full_url()
        if not self.can_fetch(url, user_agent):
            raise RobotDisallow('Error: URL is disallowed in robots.txt:'+url)
        return request

    def get_robot(self, robot_url):
        ''' fetches the appropriate robots.txt file '''

        if robot_url in self.robot_files:
            robot_file = self.robot_files[robot_url]
        else:
            # Set this url to None to catch recursion loops
            self.robot_files[robot_url] = None 
            try:
                robotstxt = self.parent.open(robot_url)
                try:
                    robot_file = RobotFileParser()
                    robot_file.parse(robotstxt.readlines())
                finally:
                    robotstxt.close()
            except (urllib2.HTTPError, urllib2.URLError):
                robot_file = None
            self.robot_files[robot_url] = robot_file
        return robot_file

    def can_fetch(self, url, user_agent=None):
        '''
        Determines whether a given url may be fetched using the given
        user_agent. Downloads the appropriate robots.txt file, as needed.

        '''

        # we're always allowed to fetch robots.txt files!
        if RobotsTxt.is_robot_url(url):
            return True

        if not user_agent:
            user_agent = '*'
        robot_url = RobotsTxt.make_robot_url(url)
        robot = self.get_robot(robot_url)

        if robot:
            return robot.can_fetch(user_agent, url)
        else:
            # if no robot file exists, this implies download consent
            return True

