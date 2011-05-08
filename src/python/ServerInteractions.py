"""
Handles client interactions with remote REST interface
"""

import json
import urllib
from urlparse import urlunparse
from httplib import HTTPConnection
#from httplib import HTTPSConnection
from httplib import HTTPException
from WMCore.Services.Requests import JSONRequests

class HTTPRequests(dict):
    """
    This code is a simplified version of WMCore.Services.Requests - we don't
    need all the bells and whistles here since data is always sent via json we
    also move the encoding of data out of the makeRequest.

    HTTPRequests does no logging or exception handling, these are managed by the
    Client class that instantiates it.

    NOTE: This class should be replaced by the WMCore.Services.JSONRequests if WMCore
    is used more in the client.
    """

    def __init__(self, url = 'localhost'):
        """
        Initialise an HTTP handler
        """
        #set up defaults
        self.setdefault("accept_type", 'text/html')
        self.setdefault("content_type", 'application/x-www-form-urlencoded')
        self.setdefault("host", url)
        # get the URL opener
        self.setdefault("conn", self.getUrlOpener())

    def getUrlOpener(self):
        """
        method getting an HTTPConnection, it is used by the constructor such
        that a sub class can override it to have different type of connection
        i.e. - if it needs authentication, or some fancy handler
        """
        #TODO: support https here
        return HTTPConnection(self['host'])

    def get(self, uri = None, data = {}):
        """
        GET some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'GET')

    def post(self, uri = None, data = {}):
        """
        POST some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'POST')

    def put(self, uri = None, data = {}):
        """
        PUT some data
        """
        return self.makeRequest(uri = uri, data = data, verb = 'PUT')

    def delete(self, uri = None, data = {}):
        """
        DELETE some data
        """
        return self.makeRequest(uri = uri, data = data)

    def makeRequest(self, uri = None, data = {}, verb = 'GET',
                     encoder = True, decoder = True, contentType = None):
        """
        Make a request to the remote database. for a give URI. The type of
        request will determine the action take by the server (be careful with
        DELETE!). Data should be a dictionary of {dataname: datavalue}.

        Returns a tuple of the data from the server, decoded using the
        appropriate method the response status and the response reason, to be
        used in error handling.

        You can override the method to encode/decode your data by passing in an
        encoding/decoding function to this method. Your encoded data must end up
        as a string.

        """
        headers = {"Content-type": "application/json",
                   "User-agent": "CRABClient/v001",
                   "Accept": "application/json"}

        if verb != 'GET' and data:
            headers["Content-length"] = len(data)
        elif verb == 'GET' and data:
            #encode the data as a get string
            uri = "%s?%s" % (uri, urllib.urlencode(data, doseq = True))
            data = {}

        self['conn'].connect()
        self['conn'].request(verb, uri, data, headers)
        response = self['conn'].getresponse()
        result = response.read()
        self['conn'].close()

        if response.status >= 400:
            e = HTTPException()
            setattr(e, 'req_data', data)
            setattr(e, 'req_headers', headers)
            setattr(e, 'url', self.buildUrl(uri))
            setattr(e, 'result', result)
            setattr(e, 'status', response.status)
            setattr(e, 'reason', response.reason)
            setattr(e, 'headers', response.getheaders())
            raise e

        #result = json.loads(result)
        return self.decodeJson(result), response.status, response.reason

    def decodeJson(self, result):
        """
        decodeJson 

        decode the response result reveiced from the server
        """
        encoder = JSONRequests()
        return encoder.decode(result)


    def buildUrl(self, uri):
        """
        Prepares the remote URL
        """
        scheme = 'http'
        if self['conn'].__class__.__name__.startswith('HTTPS'):
            scheme = 'https'
        netloc = '%s:%s' % (self['conn'].host, self['conn'].port)
        return urlunparse([scheme, netloc, uri, '', '', ''])

