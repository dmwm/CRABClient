#!/usr/bin/env python
# encoding: utf-8
"""
This contains some utility methods for the client
"""

import os
import re
import datetime
import logging

import pkgutil
import sys
import cPickle

from string import upper
from optparse import OptionValueError

from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.client_exceptions import TaskNotFoundException, CachefileNotFoundException, ConfigurationException



#if certificates in myproxy expires in less than RENEW_MYPROXY_THRESHOLD days renew them
RENEW_MYPROXY_THRESHOLD = 15

class colors:
    if sys.stdout.isatty():
        RED = '\033[91m'
        GREEN = '\033[92m'
        GRAY = '\033[90m'
        NORMAL = '\033[0m'
    else:
        NORMAL = ''
        RED = ''
        GREEN = ''
        GRAY = ''

def getPlugins(namespace, plugins, skip):
    """
    _getPlugins_

    returns a dictionary with key='class name' and value='hook to the module'
    as input needs the package name that contains the different modules

    TODO: If we use WMCore more, replace with the WMFactory.
    """
    packagemod = __import__( '%s.%s' % (namespace, plugins), \
                                        globals(), locals(), plugins  )
    fullpath   = packagemod.__path__[0]
    modules = {}
    ## iterating on the modules contained in that package
    for el in list(pkgutil.iter_modules([fullpath])):
        if el[1] not in skip:
            ## import the package module
            mod = __import__('%s.%s.%s' % (namespace, plugins, el[1]), \
                                        globals(), locals(), el[1] )
            ## add to the module dictionary.
            ## N.B. Utilitiy modules like LumiMask do not have classes inside them
            modules[el[1]] = getattr(mod, el[1], None)
            #set the default name if it has not been overridden in the class
            if not hasattr(modules[el[1]], 'name') and modules[el[1]]:
                setattr(modules[el[1]], 'name', modules[el[1]].__name__)

    return modules


def addPlugin(pluginpathname, pluginname = None):
    """
    _addPlugin_

    allows to import an external plug-in and return the dictionary here below
    {
     'plug-name': 'plug-module'
    }

    TODO: add the ability to import a specific module of the file
    """
    modules = {}
    ## file really exists?
    if os.path.exists(pluginpathname):

        ## is that really a file?
        if os.path.isfile(pluginpathname):

            ## get the file name
            pluginfilename = os.path.basename(pluginpathname)

            ## get the directory name and import it if not already
            pluginpath = os.path.dirname(pluginpathname)
            if pluginpath not in sys.path:
                sys.path.append(pluginpath)

            ## get the module name
            mod = os.path.splitext(pluginfilename)[0]

            ## import the module
            imod = __import__(mod)

            ## get the plug-in class
            ## Note: this currently need the module name = plug-in/class name
            modules[mod] = getattr(imod, mod)

    return modules


def getJobTypes(jobtypepath = 'CRABClient', jobtypename = 'JobType'):
    """
    _getJobTypes_

    wrap the dynamic plug-in import for the available job types

    TODO: this can also be a call to get a specific job type from the server
    """
    allplugins = getPlugins(jobtypepath, jobtypename, ['BasicJobType'])
    result = {}
    for k in allplugins:
        result[upper(k)] = allplugins[k]
    return result


def getAvailCommands(subcmdpath = 'CRABClient', subcmdname = 'Commands'):
    """
    _getJobTypes_

    wrap the dynamic plug-in import for the available job types

    TODO: this can also be a call to get a specific job type from the server
    """
    subcmdplugins = getPlugins(subcmdpath, subcmdname, ['SubCommand'])
    result = {}
    for k in subcmdplugins.keys():
        if subcmdplugins[k].visible:
            result[k] = subcmdplugins[k]

    return result


def getRequestName(requestName = None):
    """
    _getRequestName_

    create the directory name
    """
    prefix  = 'crab_'
    postfix = str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))

    if requestName is None or len(requestName) == 0:
        return prefix + postfix
    else:
        return prefix + requestName # + '_' + postfix


def addFileLogger(logger, workingpath, logname = 'crab.log'):
    """
    _addFileLogger_
    """
    logfullpath = os.path.join( workingpath, logname )

    # Log debug messages to crab.log file with more verbose format
    handler = logging.FileHandler( logfullpath )
    handler.setLevel(logging.DEBUG)

    ff = logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s")
    handler.setFormatter( ff )

    logger.addHandler( handler )

    # Full tracebacks should only go to the file
    # The traceback logger is also used to get messages from libraries (e.g. Proxy)
    traceback_log = logging.getLogger('CRAB3:traceback')
    traceback_log.propagate = False
    traceback_log.setLevel(logging.DEBUG) #Level to debug to get errors from Proxy library
    traceback_log.addHandler(handler)

    return logfullpath


def createWorkArea(logger, workingArea = '.', requestName = ''):
    """
    _createWorkArea_

    creates the working directory with the needed sub-folders
    in case it already exists it raises an exception
    """

    if workingArea is None or workingArea == '.' :
        workingArea = os.getenv('CRAB_WORKING_AREA', '.')

    ## create the working area if it is not there
    if not os.path.exists(workingArea):
        os.mkdir( workingArea )

    requestName = getRequestName(requestName)

    fullpath = os.path.join(workingArea, requestName)

    ## checking if there is no a duplicate
    if os.path.exists(fullpath):
        raise OSError("Working area '%s' already exists" % fullpath)

    ## creating the work area
    os.mkdir(fullpath)
    os.mkdir(os.path.join(fullpath, 'results'))
    os.mkdir(os.path.join(fullpath, 'inputs'))

    ## define the log file
    logfile = addFileLogger( logger, workingpath = fullpath )

    return fullpath, requestName, logfile


def createCache(requestarea, host, port, uniquerequestname, voRole, voGroup, instance):
    touchfile = open(os.path.join(requestarea, '.requestcache'), 'w')
    neededhandlers = {
                      "Server" : host,
                      "Port" : port,
                      "RequestName" : uniquerequestname,
                      "voRole" : voRole,
                      "voGroup" : voGroup,
                      "instance" : instance
                     }
    cPickle.dump(neededhandlers, touchfile)
    touchfile.close()


def getWorkArea( task ):
    requestarea = ''
    requestname = ''
    if os.path.isabs( task ):
        requestarea = task
        requestname = os.path.split(os.path.normpath(requestarea))[1]
    else:
        requestarea = os.path.abspath( task )
        requestname = task
    return requestarea, requestname


def loadCache( task, logger ):
    requestarea, requestname = getWorkArea( task )
    cachename = os.path.join(requestarea, '.requestcache')
    taskName = task.split('/')[-1] #Contains only the taskname without the path

    #Check if the task directory exists
    if not os.path.isdir( requestarea ):
        msg = 'Working directory for task %s not found ' % taskName
        raise TaskNotFoundException( msg )
    #If the .requestcache file exists open it!
    if os.path.isfile(cachename):
        loadfile = open( cachename, 'r' )
    else:
        msg = 'Cannot find .requestcache file inside the working directory for task %s' % taskName
        raise CachefileNotFoundException( msg )

    logfile = addFileLogger( logger, workingpath = requestarea )
    return cPickle.load(loadfile), logfile

#TODO delete initProxy (and delegate proxy) and just use CredentialInteractions in commands
def initProxy(voRole, voGroup, logger):
    proxy = CredentialInteractions(
                                    '',
                                    '',
                                    voRole,
                                    voGroup,
                                    logger
                                  )

    logger.debug("Checking credentials")
    userdn, proxyfilename = proxy.createNewVomsProxy( timeleftthreshold = 600 )
    #return also the proxy because successive proxy delegations needs to use the
    #same proxy instsance
    return userdn, proxyfilename, proxy

def getUserName(logger, voRole='', voGroup=''):
    _, _, proxy = initProxy(voRole, voGroup, logger) 
    return proxy.getUserName()

def delegateProxy(serverDN, myProxy, proxyobj, logger, nokey=False):
    proxyobj.defaultDelegation['serverDN'] = serverDN
    proxyobj.defaultDelegation['myProxySvr'] = myProxy

    logger.debug("Registering user credentials for server %s" % serverDN)
    proxyobj.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD, nokey=nokey)

def validServerURL(option, opt_str, value, parser):
    """
    This raises an optparse error if the url is not valid
    """
    if value is not None:
        if not validURL(value):
            raise OptionValueError("%s option value '%s' not valid." % (opt_str, value))
        setattr(parser.values, option.dest, value)
    else:
        setattr(parser.values, option.dest, option.default)

def validURL(serverurl, attrtohave = ['scheme', 'netloc', 'hostname'], attrtonothave = ['path', 'params', 'query', 'fragment', 'username', 'password']):
    """
    returning false if the format is different from https://host:port
    """
    tempurl = serverurl
    if not serverurl.startswith('https://'):
        tempurl = 'https://' + serverurl
    from urlparse import urlparse
    parsedurl = urlparse(tempurl)
    for elem in attrtohave:
        elemval = getattr(parsedurl, elem, None)
        if str( elemval ) == '' or elemval is None:
            return False
    for elem in attrtonothave:
        elemval = getattr(parsedurl, elem, None)
        if not str( elemval ) == '' and elemval is not None:
            return False
    return True

def validateJobids(jobids):
    #check the format of jobids
    if re.compile('^\d+(,\d+)*$').match(jobids):
        return [('jobids',jobid) for jobid in jobids.split(',')]
    else:
        raise ConfigurationException("The command line option jobids should be a comma separated list of integers")


#XXX Trying to do it as a Command causes a lot of headaches (and workaround code).
#Since server_info class needs SubCommand, and SubCommand needs server_info for
#delegating the proxy then we are screwed
#If anyone has a better solution please go on, otherwise live with that one :) :)
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import RESTCommunicationException
def server_info(subresource, server, proxyfilename, baseurl):
    """
    Get relevant information about the server
    """

    server = HTTPRequests(server, proxyfilename)

    dictresult, status, reason = server.get(baseurl, {'subresource' : subresource})

    return dictresult['result'][0]
