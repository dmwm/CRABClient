"""
This module contains the utility methods available for users.
"""

import os
import logging
import traceback
import subprocess
from urlparse import urlparse
from httplib import HTTPException

## DBS dependencies
from dbs.apis.dbsClient import DbsApi

## WMCore dependencies
from WMCore.Configuration import Configuration
from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.pycurl_manager import RequestHandler

## CRAB dependencies
from RESTInteractions import HTTPRequests
from CRABClient.ClientUtilities import DBSURLS, LOGLEVEL_MUTE, colors
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
    """
    parsedUrl = urlparse(url)
    if filename == None:
        path = parsedUrl.path
        filename = os.path.basename(path)

    data = getDataFromURL(url, proxyfilename)

    if data:
        try:
            with open(filename, 'a') as f:
                f.seek(0)
                f.truncate()
                f.write(data)
        except IOError as ex:
            logger = logging.getLogger('CRAB3')
            logger.exception(ex)
            msg = "Error while writing %s. Got:\n%s" \
                    % (filename, ex)
            raise ClientException(msg)

    return filename


def getDataFromURL(url, proxyfilename = None):
    """
    Read the content of a URL and return it as a string.
    Type of content should not matter, it can be a json file or a tarball for example.

    url: the link you would like to retrieve
    proxyfilename: the x509 proxy certificate to be used in case auth is required

    Returns binary data encoded as a string, which can be later processed
    according to what kind of content it represents.
    """

    # Get rid of unicode which may cause problems in pycurl
    stringUrl = url.encode('ascii')

    reqHandler = RequestHandler()
    try:
        _, data = reqHandler.request(url=stringUrl, params={}, ckey=proxyfilename,
                cert=proxyfilename, capath=HTTPRequests.getCACertPath())
    except HTTPException as ex:
        raise ClientException(ex)

    return data


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

def getMutedStatusInfo(logger):
    """
    Mute the status console output before calling status and change it back to normal afterwards.
    """
    mod = __import__('CRABClient.Commands.status', fromlist='status')
    cmdobj = getattr(mod, 'status')(logger)
    loglevel = getConsoleLogLevel()
    setConsoleLogLevel(LOGLEVEL_MUTE)
    statusDict = cmdobj.__call__()
    setConsoleLogLevel(loglevel)

    if statusDict['statusFailureMsg']:
        # If something happens during status execution we still want to print it
        logger.error("Error while getting status information. Got:\n%s " %
                          statusDict['statusFailureMsg'])

    return statusDict

def getColumn(dictresult, columnName):
    columnIndex = dictresult['desc']['columns'].index(columnName)
    value = dictresult['result'][columnIndex]
    if value=='None':
        return None
    else:
        return value
