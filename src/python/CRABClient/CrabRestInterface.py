"""
Handles client interactions with remote REST interface
"""

from __future__ import division
from __future__ import print_function

import os
import random
from CRABClient.ClientUtilities import execute_command
from ServerUtilities import encodeRequest
import json
import re

from CRABClient.ClientExceptions import RESTInterfaceException

try:
    from urllib import quote as urllibQuote  # Python 2.X
except ImportError:
    from urllib.parse import quote as urllibQuote  # Python 3+

import logging

try:
    from TaskWorker import __version__
except:  # pylint: disable=bare-except
    try:
        from CRABClient import __version__
    except:  # pylint: disable=bare-except
        __version__ = '0.0.0'


EnvironmentException = Exception

def retriableError(http_code, curlExitCode):
    """
        checks if error is worth retrying
        :param http_code: int : HTTP code form the HTTP call if it was possible to do the call and obtain it
        :param curlExitCode: int : exit code of the curl command that was forked to execute the HTTP call
        :return: True of False indicating if the error is worth retrying
    """
    retry = False

    #429 Too Many Requests. When client hits the throttling limit
    #500 Internal sever error. For some errors retries it helps
    #502 CMSWEB frontend answers with this when the CMSWEB backends are overloaded
    #503 Usually that's the DatabaseUnavailable error

    #28 is 'Operation timed out...'
    #35,is 'Unknown SSL protocol error', see https://github.com/dmwm/CRABServer/issues/5102

    if http_code in [429, 500, 502, 503] or curlExitCode in [28, 35]:
        retry = True

    return retry


class HTTPRequests(dict):
    """
    This code forks a subprocess which executes curl to communicate
    with CRAB REST.
    """

    def __init__(self, hostname='localhost', localcert=None, localkey=None, version=__version__,
                 retry=0, logger=None, verbose=False, userAgent='CRAB?'):
        """
        Initialise an HTTP handler
        """
        dict.__init__(self)
        # set up defaults
        self.setdefault("host", hostname)
        # setup port 8443 for cmsweb services (leave them alone things like personal private VM's)
        if self['host'].startswith("https://cmsweb") or self['host'].startswith("cmsweb"):
            if self['host'].endswith(':8443'):
                # good to go
                pass
            elif ':' in self['host']:
                # if there is a port number already, trust it
                pass
            else:
                # add port 8443
                self['host'] = self['host'].replace(".cern.ch", ".cern.ch:8443", 1)
        self.setdefault("cert", localcert)
        self.setdefault("key", localkey)
        self.setdefault("version", version)
        self.setdefault("retry", retry)
        self.setdefault("verbose", verbose)
        self.setdefault("userAgent", userAgent)
        self.logger = logger if logger else logging.getLogger()

    def get(self, uri=None, data=None):
        """
        GET some data
        """
        return self.makeRequest(uri=uri, data=data, verb='GET')

    def post(self, uri=None, data=None):
        """
        POST some data
        """
        return self.makeRequest(uri=uri, data=data, verb='POST')

    def put(self, uri=None, data=None):
        """
        PUT some data
        """
        return self.makeRequest(uri=uri, data=data, verb='PUT')

    def delete(self, uri=None, data=None):
        """
        DELETE some data
        """
        return self.makeRequest(uri=uri, data=data, verb='DELETE')

    def makeRequest(self, uri=None, data=None, verb='GET'):
        """
        Make a request to the remote database for a given URI. The type of
        request will determine the action taken by the server (be careful with
        DELETE!).

        Returns a tuple of the data from the server, decoded using the
        appropriate method, the response status and the response reason, to be
        used in error handling.

        You can override the method to encode/decode your data by passing in an
        encoding/decoding function to this method. Your encoded data must end up
        as a string.
        """

        data = data or {}

        # Quoting the uri since it can contain the request name, and therefore spaces (see #2557)
        uri = urllibQuote(uri)
        caCertPath = self.getCACertPath()
        url = 'https://' + self['host'] + uri

        # if it is a dictionary, we need to encode it to string
        if isinstance(data, dict):
            data = encodeRequest(data)

        if verb in ['GET', 'HEAD']:
            url = url + '?' + data

        command = ''
        # command below will return 2 values separated by comma: 1) curl result and 2) HTTP code
        command += 'curl -v -X {0}'.format(verb)
        command += ' -H "User-Agent: %s/%s"' % (self['userAgent'], self['version'])
        command += ' -H "Accept: */*"'
        command += ' --data "%s"' % data
        command += ' --cert "%s"' % self['cert']
        command += ' --key "%s"' % self['key']
        command += ' --capath "%s"' % caCertPath
        command += ' "%s" | tee /dev/stderr ' % url

        # retries this up at least 3 times, or up to self['retry'] times for range of exit codes
        # retries are counted AFTER 1st try, so call is made up to nRetries+1 times !
        nRetries = max(2, self['retry'])
        for i in range(nRetries + 1):
            stdout, stderr, curlExitCode = execute_command(command=command, logger=self.logger)

            http_code, http_reason = 99999, ''
            http_response = re.search(r'(?<=\<\sHTTP/1.1\s)[^\n]*',stderr)
            if http_response is not None:
                http_code, http_reason = http_response.group(0).split(" ", 1)
                http_code = int(http_code)
            if curlExitCode != 0 or http_code != 200:
                if (i < 2) or (retriableError(http_code, curlExitCode) and (i < self['retry'])):
                    sleeptime = 20 * (i + 1) + random.randint(-10, 10)
                    msg = "Sleeping %s seconds after HTTP error.\nError:\n:%s" % (sleeptime, stderr)
                    self.logger.debug(msg)
                else:
                    # this was the last retry
                    msg = "Fatal error trying to connect to %s using %s." % (url, data)
                    self.logger.info(msg)
                    raise RESTInterfaceException(stderr)
            else:
                try:
                    curlResult = json.loads(stdout)
                except Exception as ex:
                    msg = "Fatal error reading data from %s using %s: \n%s" % (url, data, ex)
                    raise Exception(msg)
                else:
                    break

        return curlResult, http_code, http_reason

    @staticmethod
    def getCACertPath():
        """ Get the CA certificate path. It looks for it in the X509_CERT_DIR variable if present
            or return /etc/grid-security/certificates/ instead (if it exists)
            If a CA certificate path cannot be found throws a EnvironmentException exception
        """
        caDefault = '/etc/grid-security/certificates/'
        if "X509_CERT_DIR" in os.environ:
            return os.environ["X509_CERT_DIR"]
        if os.path.isdir(caDefault):
            return caDefault
        raise EnvironmentException(
            "The X509_CERT_DIR variable is not set and the %s directory cannot be found.\n" % caDefault +
            "Cannot find the CA certificate path to authenticate the server.")


class CRABRest:
    """
    A convenience class to communicate with CRABServer REST
    Encapsulates an HTTPRequest object (which can be used also with other HTTP servers)
    together with the CRAB DB instance and allows to specify simply the CRAB Server API in
    the various HTTP methods.

    Add two methods to set and get the DB instance
    """

    def __init__(self, hostname='localhost', localcert=None, localkey=None, version=__version__,
                 retry=0, logger=None, verbose=False, userAgent='CRAB?'):
        self.server = HTTPRequests(hostname, localcert, localkey, version,
                                   retry, logger, verbose, userAgent)
        instance = 'prod'
        self.uriNoApi = '/crabserver/' + instance + '/'

    def setDbInstance(self, dbInstance='prod'):
        self.uriNoApi = '/crabserver/' + dbInstance + '/'

    def getDbInstance(self):
        return self.uriNoApi.rstrip('/').split('/')[-1]

    def get(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.get(uri, data)

    def post(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.post(uri, data)

    def put(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.put(uri, data)

    def delete(self, api=None, data=None):
        uri = self.uriNoApi + api
        return self.server.delete(uri, data)

