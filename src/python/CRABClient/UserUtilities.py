"""
This module contains the utility methods available for users.
"""

import os
import logging
import logging.handlers
import commands
import string
import urllib
from urlparse import urlparse

## WMCore dependencies
from WMCore.Configuration import Configuration

## CRAB dependencies
from CRABClient.ClientExceptions import ClientException, UsernameException, ProxyException


def config():
    """
    Return a Configuration object containing all the sections that CRAB recognizes.
    """
    config = Configuration()
    config.section_("General")
    config.section_("JobType")
    config.section_("Data")
    config.section_("Site")
    config.section_("User")
    config.section_("Debug")
    return config


def getUsernameFromSiteDB():
    """
    Retrieve username from SiteDB by doing a query to
    https://cmsweb.cern.ch/sitedb/data/prod/whoami
    using the users proxy.
    """
    scram_cmd = 'which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`'
    ## Check if there is a proxy.
    status, proxy_info = commands.getstatusoutput(scram_cmd+'; voms-proxy-info')
    if status:
        raise ProxyException(proxy_info)
    ## Check if proxy is valid.
    status, proxy_time_left = commands.getstatusoutput(scram_cmd+'; voms-proxy-info -timeleft')
    if status or not proxy_time_left:
        msg  = "Aborting the attempt to retrieve username from SiteDB:"
        msg += " Command 'voms-proxy-info -timeleft' returned status %s and output '%s'." % (status, proxy_time_left)
        raise ProxyException(msg)
    if int(proxy_time_left) < 60:
        msg  = "Aborting the attempt to retrieve username from SiteDB:"
        msg += " Proxy not valid or valid for less than 60 seconds."
        raise ProxyException(msg)
    ## Retrieve proxy file location.
    ## (Redirect stderr from voms-proxy-* to dev/null to avoid stupid Java messages)
    status, proxy_file_name = commands.getstatusoutput(scram_cmd+'; voms-proxy-info -path 2>/dev/null')
    if status or not proxy_file_name:
        msg  = "Aborting the attempt to retrieve username from SiteDB:"
        msg += " Command 'voms-proxy-info -path' returned status %s and output '%s'." % (status, proxy_file_name)
        raise ProxyException(msg)
    ## Path to certificates.
    capath = os.environ['X509_CERT_DIR'] if 'X509_CERT_DIR' in os.environ else '/etc/grid-security/certificates'
    ## Prepare command to query username from SiteDB.
    cmd  = "curl -s --capath %s --cert %s --key %s 'https://cmsweb.cern.ch/sitedb/data/prod/whoami'" % (capath, proxy_file_name, proxy_file_name)
    cmd += " | tr ':,' '\n'"
    cmd += " | grep -A1 login"
    cmd += " | tail -1"
    ## Execute the command.
    status, username = commands.getstatusoutput(cmd)
    if status or not username:
        msg  = "Unable to retrieve username from SiteDB:" ## don't change this message, or change also checkusername.py
        msg += " Command '%s' returned status %s and output '%s'." % (cmd.replace('\n', '\\n'), status, username)
        raise UsernameException(msg)
    username = string.strip(username).replace('"','')
    if username == 'null':
        username = None
    return username


def getFileFromURL(url, filename = None):
    """
    Read the content of a URL and copy it into a file.
    """
    if filename == None:
        path = urlparse(url).path
        filename = os.path.basename(path)
    try:
        socket = urllib.urlopen(url)
        filestr = socket.read()
    except IOError, ioex:
        tblogger = logging.getLogger('CRAB3')
        tblogger.exception(ioex)
        msg = "Error while trying to retrieve file from %s: %s" % (url, ioex)
        msg += "\nMake sure the URL is correct."
        raise ClientException(msg)
    except Exception, ex:
        tblogger = logging.getLogger('CRAB3')
        tblogger.exception(ex)
        msg = 'Unexpected error while trying to retrieve file from %s: %s' % (url, ex)
        raise ClientException(msg)
    with open(filename, 'w') as f:
        f.write(filestr)
    return filename

