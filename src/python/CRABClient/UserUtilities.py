"""
This module contains the utility methods available for users.
"""

import os
import urllib
import logging
import traceback
import subprocess
from urlparse import urlparse

## DBS dependencies
from dbs.apis.dbsClient import DbsApi

## WMCore dependencies
from WMCore.Configuration import Configuration
from WMCore.DataStructs.LumiList import LumiList

## CRAB dependencies
from CRABClient.ClientUtilities import DBSURLS, colors
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
    scram_cmd = "which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`"
    ## Check if there is a proxy.
    cmd = scram_cmd + "; voms-proxy-info"
    process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Aborting the attempt to retrieve username from SiteDB."
        msg += "\nDetails follow:"
        msg += "\n  Error executing command: %s" % (cmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Stderr:\n      %s" % (str(stderr).replace('\n', '\n      '))
        raise ProxyException(msg)
    ## Check if proxy is valid.
    #proxyTimeLeft = [x[x.find(':')+2:] for x in stdout.split('\n') if 'timeleft' in x][0]
    cmd = scram_cmd + "; voms-proxy-info -timeleft"
    process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Aborting the attempt to retrieve username from SiteDB."
        msg += "\nDetails follow:"
        msg += "\n  Error executing command: %s" % (cmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Stderr:\n      %s" % (str(stderr).replace('\n', '\n      '))
        raise ProxyException(msg)
    proxyTimeLeft = str(stdout).replace('\n','')
    if int(proxyTimeLeft) < 60:
        msg  = "Aborting the attempt to retrieve username from SiteDB."
        msg += "\nProxy time left = %s seconds. Please renew your proxy." % (proxyTimeLeft)
        raise ProxyException(msg)
    ## Retrieve proxy file location.
    cmd = scram_cmd + "; voms-proxy-info -path"
    process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Aborting the attempt to retrieve username from SiteDB."
        msg += "\nDetails follow:"
        msg += "\n  Error executing command: %s" % (cmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Stderr:\n      %s" % (str(stderr).replace('\n', '\n      '))
        raise ProxyException(msg)
    proxyFileName = str(stdout).replace('\n','')
    ## Path to certificates.
    capath = os.environ['X509_CERT_DIR'] if 'X509_CERT_DIR' in os.environ else "/etc/grid-security/certificates"
    ## Retrieve user info from SiteDB.
    queryCmd = "curl -s --capath %s --cert %s --key %s 'https://cmsweb.cern.ch/sitedb/data/prod/whoami'" % (capath, proxyFileName, proxyFileName)
    process = subprocess.Popen(queryCmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Error contacting SiteDB."
        msg += "\nDetails follow:"
        msg += "\n  Executed command: %s" % (queryCmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Stderr:\n      %s" % (str(stderr).replace('\n', '\n      '))
        raise UsernameException(msg)
    ## Extract the username from the above command output.
    parseCmd = "echo '%s' | tr ':,' '\n' | grep -A1 login | tail -1 | tr -d ' \n\"'" % (str(stdout))
    process = subprocess.Popen(parseCmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    username, stderr = process.communicate()
    if username == 'null' or not username:
        msg  = "Failed to retrieve username from SiteDB. Your DN does not seem to be registered in SiteDB."
        msg += "\nDetails follow:"
        msg += "\n  Executed command: %s" % (queryCmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Parsed username: %s" % (username)
        msg += "\n%sNote%s: Make sure you have the correct certificate mapped in SiteDB" % (colors.BOLD, colors.NORMAL)
        msg += " (you can check what is the certificate you currently have mapped in SiteDB"
        msg += " by searching for your name in https://cmsweb.cern.ch/sitedb/prod/people)."
        msg += " For instructions on how to map a certificate in SiteDB, see https://twiki.cern.ch/twiki/bin/viewauth/CMS/SiteDBForCRAB."
        raise UsernameException(msg)
    return username


def getFileFromURL(url, filename = None, proxyfilename = None):
    """
    Read the content of a URL and copy it into a file.

    url: the link you would like to retrieve
    filename: the local filename where the url is saved to. Defaults to the filename in the url
    proxyfilename: the x509 proxy certificate to be used in case auth is required

    Return the filename used to save the file or raises ClientException in case of errors (a status attribute is added if the error is an http one).
    """
    parsedurl = urlparse(url)
    if filename == None:
        path = parsedurl.path
        filename = os.path.basename(path)
    try:
        opener = urllib.URLopener(key_file = proxyfilename, cert_file = proxyfilename)
        socket = opener.open(url)
        status = socket.getcode()
        filestr = socket.read()
    except IOError as ioex:
        msg = "Error while trying to retrieve file from %s: %s" % (url, ioex)
        msg += "\nMake sure the URL is correct."
        exc = ClientException(msg)
        if ioex[0] == 'http error':
            exc.status = ioex[1]
        raise exc
    except Exception as ex:
        tblogger = logging.getLogger('CRAB3')
        tblogger.exception(ex)
        msg = "Unexpected error while trying to retrieve file from %s: %s" % (url, ex)
        raise ClientException(msg)
    if status != 200 and parsedurl.scheme in ['http', 'https']:
        exc = ClientException("Unable to retieve the file from %s. HTTP status code %s. HTTP content: %s" % (url, status, socket.info()))
        exc.status = status
        raise exc
    with open(filename, 'w') as f:
        f.write(filestr)
    return filename


def getLumiListInValidFiles(dataset, dbsurl = 'phys03'):
    """
    Get the runs/lumis in the valid files of a given dataset.

    dataset: the dataset name as published in DBS
    dbsurl: the DBS URL or DBS prod instance

    Returns a LumiList object.
    """
    dbsurl = DBSURLS['reader'].get(dbsurl, dbsurl)
    dbs3api = DbsApi(url=dbsurl)
    try:
        files = dbs3api.listFileArray(dataset=dataset, validFileOnly=0, detail=True)
    except Exception as ex:
        msg  = "Got DBS client error requesting details of dataset '%s' on DBS URL '%s': %s" % (dataset, dbsurl, ex)
        msg += "\n%s" % (traceback.format_exc())
        raise ClientException(msg)
    if not files:
        msg = "Dataset '%s' not found in DBS URL '%s'." % (dataset, dbsurl)
        raise ClientException(msg)
    validFiles = [f['logical_file_name'] for f in files if f['is_file_valid']]
    blocks = set([f['block_name'] for f in files])
    runLumiPairs = []
    for blockName in blocks:
        fileLumis = dbs3api.listFileLumis(block_name=blockName)
        for f in fileLumis:
            if f['logical_file_name'] in validFiles:
                run = f['run_num']
                lumis = f['lumi_section_num']
                for lumi in lumis:
                    runLumiPairs.append((run,lumi))
    lumiList = LumiList(lumis=runLumiPairs)
    
    return lumiList


def getLoggers():
    from CRABClient.ClientUtilities import LOGGERS
    return LOGGERS


def getConsoleLogLevel():
    from CRABClient.ClientUtilities import CONSOLE_LOGLEVEL
    return CONSOLE_LOGLEVEL


def setConsoleLogLevel(lvl):
    from CRABClient.ClientUtilities import setConsoleLogLevelVar
    setConsoleLogLevelVar(lvl)
    if 'CRAB3.all' in logging.getLogger().manager.loggerDict:
        for h in logging.getLogger('CRAB3.all').handlers:
            h.setLevel(lvl)

