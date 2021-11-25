"""
This module contains the utility methods available for users.
"""

import os
import logging
import traceback

from FWCore.PythonUtilities.LumiList import LumiList

## CRAB dependencies
from CRABClient.ClientUtilities import DBSURLS, LOGLEVEL_MUTE, colors
from CRABClient.ClientUtilities import execute_command, getUserProxy
from CRABClient.ClientExceptions import ClientException, UsernameException
from WMCore.Configuration import Configuration

def config():
    """
    Return a Configuration object containing all the sections that CRAB recognizes.
    """
    config = Configuration()  # pylint: disable=redefined-outer-name
    config.section_("General")
    config.section_("JobType")
    config.section_("Data")
    config.section_("Site")
    config.section_("User")
    config.section_("Debug")
    return config

def getUsername(proxyFile=None, logger=None):
    """
    get globally unique username to be used for this CRAB work
    this is a generic high level function which can be called even w/o arguments
    and will figure out the username from the current authentication credential
    found in the environment.
    Yet it allows the called to guide it via optional argument to be quicker
    and easier to tune to different authentication systemd (X509 now, tokens later e.g.)
    :param proxyFile: the full path of the file containing the X509 VOMS proxy, if missing
    :param logger: a logger object to use for messages, if missing, it will report to standard logger
    :return: username : a string
    """

    if not logger: logger=logging.getLogger()
    logger.debug("Retrieving username ...")

    from CRABClient.ClientUtilities import getUsernameFromCRIC_wrapped
    if not proxyFile:
        proxyFile = '/tmp/x509up_u%d'%os.getuid() if 'X509_USER_PROXY' not in os.environ else os.environ['X509_USER_PROXY']
    username = getUsernameFromCRIC_wrapped(logger, proxyFile, quiet=True)
    if username:
        logger.debug("username is %s", username)
    else:
        msg = "%sERROR:%s CRIC could not resolve the DN in the user proxy into a user name" \
              % (colors.RED, colors.NORMAL)
        msg += "\n Please find below details of failures for investigation:"
        logger.error(msg)
        username = getUsernameFromCRIC_wrapped(logger, proxyFile, quiet=False)

    return username

def getUsernameFromCRIC(proxyFileName=None):
    """
    Retrieve username from CRIC by doing a query to
    https://cms-cric.cern.ch/api/accounts/user/query/?json&preset=whoami
    using the users proxy.
    args:
    proxyfile : string : the full patch to the file containing the user proxy
    """

    ## Path to certificates.
    capath = os.environ['X509_CERT_DIR'] if 'X509_CERT_DIR' in os.environ else "/etc/grid-security/certificates"
    # Path to user proxy
    if not proxyFileName:
        proxyFileName = getUserProxy()
    if not proxyFileName:
        msg = "Can't find user proxy file"
        raise UsernameException(msg)
    ## Retrieve user info from CRIC. Note the curl must be executed in same env. (i.e. CMSSW) as crab
    queryCmd = "curl -sS --capath %s --cert %s --key %s 'https://cms-cric.cern.ch/api/accounts/user/query/?json&preset=whoami'" %\
               (capath, proxyFileName, proxyFileName)
    stdout, stderr, rc = execute_command(queryCmd)
    if rc or not stdout:
        msg  = "Error contacting CRIC."
        msg += "\nDetails follow:"
        msg += "\n  Executed command: %s" % (queryCmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Stderr:\n      %s" % (str(stderr).replace('\n', '\n      '))
        raise UsernameException(msg)
    ## Extract the username from the above command output.
    parseCmd = "echo '%s' | tr ':,' '\n' | grep -A1 login | tail -1 | tr -d ' \n\"'" % (str(stdout))
    username, stderr, rc = execute_command(parseCmd)
    if username == 'null' or not username:
        msg  = "Failed to retrieve username from CRIC."
        msg += "\nDetails follow:"
        msg += "\n  Executed command: %s" % (queryCmd)
        msg += "\n    Stdout:\n      %s" % (str(stdout).replace('\n', '\n      '))
        msg += "\n    Parsed username: %s" % (username)
        msg += "\n%sNote%s: Make sure you have the correct certificate mapped in your CERN account page" % (colors.BOLD, colors.NORMAL)
        msg += " (you can check what is the certificate you currently have mapped"
        msg += " by looking at CERN Certificatiom Authority page."
        msg += "\nFor instructions on how to map a certificate, see "
        msg += "\n  https://twiki.cern.ch/twiki/bin/view/CMSPublic/UsernameForCRAB#Adding_your_DN_to_your_profile"
        raise UsernameException(msg)
    return username

def curlGetFileFromURL(url, filename = None, proxyfilename = None, logger=None):
    """
    Read the content of a URL into a file via curl

    url: the link you would like to retrieve
    filename: the local filename where the url is saved to. Defaults to the filename in the url
    proxyfilename: the x509 proxy certificate to be used in case auth is required
    returns: the exit code of the command if command failed, otherwise the HTTP code of the call
             note that curl exits with status 0 if the HTTP calls fail,
    """

    ## Path to certificates.
    capath = os.environ['X509_CERT_DIR'] if 'X509_CERT_DIR' in os.environ else "/etc/grid-security/certificates"

    # send curl output to file and http_code to stdout
    downloadCommand = 'curl -sS --capath %s --cert %s --key %s -o %s -w %%"{http_code}"' %\
                      (capath, proxyfilename, proxyfilename, filename)
    downloadCommand += ' "%s"' % url
    if logger:
        logger.debug("Will execute:\n%s", downloadCommand)
    stdout, stderr, rc = execute_command(downloadCommand, logger=logger)
    errorDetails = ''

    if rc != 0:
        os.unlink(filename)
        httpCode = 503
    else:
        httpCode = int(stdout)
        if httpCode != 200:
            with open(filename) as f:
                errorDetails = f.read()
            os.unlink(filename)
    if logger:
        logger.debug('exitcode: %s\nstdout: %s\nstderr: %s\nerror details: %s', rc, stdout, stderr, errorDetails)
	
    return httpCode


def getLumiListInValidFiles(dataset, dbsurl = 'phys03'):
    """
    Get the runs/lumis in the valid files of a given dataset.

    dataset: the dataset name as published in DBS
    dbsurl: the DBS URL or DBS prod instance

    Returns a LumiList object.
    """

    from dbs.apis.dbsClient import DbsApi

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
