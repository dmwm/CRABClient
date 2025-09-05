# pylint: disable=consider-using-f-string
"""
Handles client interactions with remote REST interface
"""

from __future__ import division
from __future__ import print_function

import os
import random
import json
import re
import tempfile
import logging

try:
    from urllib import quote as urllibQuote  # Python 2.X
except ImportError:
    from urllib.parse import quote as urllibQuote  # Python 3+

import http

from CRABClient.ClientUtilities import execute_command
from ServerUtilities import encodeRequest
from CRABClient.ClientExceptions import RESTInterfaceException, ConfigurationException

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


def parseResponseHeader(response):
    """
    Parse response header and return HTTP code with reason
    Example taken from WMCore pycurl_manager
    """
    startRegex = r"HTTP/\d(?:\.\d)?\s\d{3}[^\n]*"
    continueRegex = r"HTTP/\d(?:\.\d)?\s100[^\n]*"  # Continue: client should continue its request
    replaceRegex = r"HTTP/\d(?:\.\d)?"

    reason = ''
    code = 9999
    for row in response.split('\r'):
        row = row.replace('\n', '')
        if not row:
            continue
        response = re.search(startRegex, row)
        if response:
            if re.search(continueRegex, row):
                continue
            res = re.sub(replaceRegex, "", response.group(0)).strip()
            parts = res.split(' ', 1)
            code = int(parts[0])
            reason = parts[1] if len(parts) > 1 else http.HTTPStatus(code).phrase
    return code, reason


class HTTPRequests(dict):
    """
    This code forks a subprocess which executes curl to communicate
    with CRAB or other REST servers which returns JSON
    """

    def __init__(self, hostname='localhost', localcert=None, localkey=None, contentType=None,
                 retry=0, logger=None, version=__version__, verbose=False, userAgent=None):
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
        if not userAgent:
            userAgent = 'CRABClient/%s' % __version__
        self.setdefault("cert", localcert)
        self.setdefault("key", localkey)
        self.setdefault("retry", retry)
        self.setdefault("verbose", verbose)
        self.setdefault("userAgent", userAgent)
        self.setdefault("Content-type", contentType)
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

        # if it is a dictionary, we need to encode it to string (will not affect JSON)
        if isinstance(data, dict):
            data = encodeRequest(data)
        self.logger.debug("Encoded data for curl request: %s", data)

        fh, path = tempfile.mkstemp(dir='/tmp', prefix='crab_curlData')
        os.close(fh)  # fh handle is for binary write and inconvenient to use
        with open(path, 'w') as f:
            f.write(data)

        if verb in ['GET', 'HEAD']:
            url = url + '?' + data

        command = ''

        # CRAB_useGoCurl env. variable is used to define how request should be executed
        # If variable is set, then goCurl is used for command execution: https://github.com/vkuznet/gocurl
        # Same variable is also used inside CRABServer, we should keep name changes (if any) synchronized
        if os.getenv('CRAB_useGoCurl'):
            command += '/cvmfs/cms.cern.ch/cmsmon/gocurl -verbose 2 -method {0}'.format(verb)
            command += ' -header "User-Agent: %s"' % self['userAgent']
            command += ' -header "Accept: */*"'
            if self['Content-type']:
                command += ' -header "Content-type: %s"' % self['Content-type']
            command += ' -data "@%s"' % path
            command += ' -cert "%s"' % self['cert']
            command += ' -key "%s"' % self['key']
            command += ' -capath "%s"' % caCertPath
            command += ' -url "%s" | tee /dev/stderr ' % url
        else:
            command += 'curl -v -X {0}'.format(verb)
            command += ' -H "User-Agent: %s"' % self['userAgent']
            command += ' -H "Accept: */*"'
            if self['Content-type']:
                command += ' -H "Content-type: %s"' % self['Content-type']
            command += ' --data @%s' % path
            command += ' --cert "%s"' % self['cert']
            command += ' --key "%s"' % self['key']
            command += ' --capath "%s"' % caCertPath
            command += ' "%s" | tee /dev/stderr ' % url

        # retries this up at least 3 times, or up to self['retry'] times for range of exit codes
        # retries are counted AFTER 1st try, so call is made up to nRetries+1 times !
        nRetries = max(2, self['retry'])
        for i in range(nRetries + 1):
            curlLogger = self.logger if self['verbose'] else None
            stdout, stderr, curlExitCode = execute_command(command=command, logger=curlLogger)
            http_code, http_reason = parseResponseHeader(stderr)

            if curlExitCode != 0 or http_code != 200:
                if (i < 2) or (retriableError(http_code, curlExitCode) and (i < self['retry'])):
                    sleeptime = 20 * (i + 1) + random.randint(-10, 10)
                    msg = "Sleeping %s seconds after HTTP error.\nError:\n:%s" % (sleeptime, stderr)
                    self.logger.debug(msg)
                else:
                    # this was the last retry
                    msg = "Fatal error trying to connect to %s using %s." % (url, data)
                    msg += "\nexit code from curl = %s" % curlExitCode
                    msg += "\nHTTP code/reason = %s/%s ." % (http_code, http_reason)
                    msg += "  stdout:\n%s" % stdout
                    self.logger.info(msg)
                    os.remove(path)
                    raise RESTInterfaceException(stderr)
            else:
                try:
                    curlResult = json.loads(stdout)
                    break
                except Exception as ex:
                    msg = "Fatal error reading data from %s using %s: \n%s" % (url, data, ex)
                    raise Exception(msg)
                finally:
                    os.remove(path)

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

    def __init__(self, hostname='localhost', localcert=None, localkey=None,
                 retry=0, logger=None, verbose=False, userAgent=None):
        self.server = HTTPRequests(hostname=hostname, localcert=localcert, localkey=localkey,
                                   retry=retry, logger=logger, verbose=verbose, userAgent=userAgent)
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


def getDbsREST(instance=None, logger=None, cert=None, key=None, userAgent=None):
    """
    given a DBS istance (e.g. prod/phys03) returns a DBSReader and DBSWriter
    HTTP client instances which can communicate with DBS REST via curl
    Arguments:
    instance: a DBS instance in the form prod/global or prod/phys03 or similar
            or a full DBS URL like https:cmsweb.cern.ch/dbs/dev/phys01/DBSReader
            in the latter care the corresponding DBSWriter will also be created, or
            viceversa, the caller can indicate a DBSWriter and will get back HTTPRequest
            objects for both Reader and Writer
    logger: a logger
    cert, key : name of files, can use the path to X509_USER_PROXY for both
    """
    # if user supplied a simple prod/phys03 like instance, these two lines will do
    # note that our HTTPRequests will add https://
    dbsReadUrl = "cmsweb.cern.ch:8443/dbs/" + instance + "/DBSReader/"
    dbsWriteUrl = "cmsweb.cern.ch:8443/dbs/" + instance + "/DBSWriter/"
    # a possible use case e.g. for testing is to use int instance of DBS. requires testbed CMSWEB
    if instance.startswith('int'):
        dbsReadUrl = dbsReadUrl.replace('cmsweb', 'cmsweb-testbed')
        dbsWriteUrl = dbsWriteUrl.replace('cmsweb', 'cmsweb-testbed')
    # if user knoww better and provided a full URL, we'll take and adapt
    # to have both Reader and Writer,
    if instance.startswith("https://"):
        url = instance.lstrip("https://")  # will be added back in HTTPRequests
        if "DBSReader" in url:
            dbsReadUrl = url
            dbsWriteUrl = url.replace('DBSReader', 'DBSWriter')
        elif 'DBSWriter' in url:
            dbsWriteUrl = url
            dbsReadUrl = url.replace('DBSWriter', 'DBSReader')
        else:
            raise ConfigurationException("bad instance value %s" % instance)

    logger.debug('Read Url  = %s' % dbsReadUrl)
    logger.debug('Write Url = %s' % dbsWriteUrl)

    dbsReader = HTTPRequests(hostname=dbsReadUrl, localcert=cert, localkey=key,
                             contentType='application/json',
                             retry=2, logger=logger, verbose=False, userAgent=userAgent)

    dbsWriter = HTTPRequests(hostname=dbsWriteUrl, localcert=cert, localkey=key,
                             contentType='application/json',
                             retry=2, logger=logger, verbose=False, userAgent=userAgent)
    return dbsReader, dbsWriter
