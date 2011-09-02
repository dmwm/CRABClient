#!/usr/bin/env python
# encoding: utf-8
"""
This contains some utility methods for the client
"""

import os
import datetime
import logging

import pkgutil
import sys
import cPickle

from string import upper

from CredentialInteractions import CredentialInteractions
from client_exceptions import TaskNotFoundException, CachefileNotFoundException

from optparse import OptionValueError


def getPlugins(plugins, skip):
    """
    _getPlugins_

    returns a dictionary with key='class name' and value='hook to the module'
    as input needs the package name that contains the different modules

    TODO: If we use WMCore more, replace with the WMFactory.
    """
    packagemod = __import__( plugins )
    fullpath   = packagemod.__path__[0]

    modules = {}
    ## iterating on the modules contained in that package
    for el in list(pkgutil.iter_modules([fullpath])):
        if el[1] not in skip:
            ## import the package module
            mod = __import__('%s.%s' % (plugins, el[1]), \
                                        globals(), locals(), el[1] )
            ## add to the module dictionary
            modules[el[1]] = getattr(mod, el[1])

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


def getJobTypes(jobtypepath = 'JobType'):
    """
    _getJobTypes_

    wrap the dynamic plug-in import for the available job types

    TODO: this can also be a call to get a specific job type from the server
    """
    allplugins = getPlugins(jobtypepath, ['BasicJobType'])
    result = {}
    for k in allplugins:
        result[upper(k)] = allplugins[k]
    return result


def getAvailCommands(subcmdpath = 'Commands'):
    """
    _getJobTypes_

    wrap the dynamic plug-in import for the available job types

    TODO: this can also be a call to get a specific job type from the server
    """

    subcmdplugins = getPlugins(subcmdpath, ['SubCommand'])
    result = {}
    for k in subcmdplugins.keys():
        if subcmdplugins[k].visible:
            result[subcmdplugins[k].name] = {'names': subcmdplugins[k].names, 'module': subcmdplugins[k]}

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
    logger.debug("Setting log file %s " % logfullpath)

    # Log debug messages to crab.log file with more verbose format

    handler = logging.FileHandler( logfullpath )
    handler.setLevel(logging.DEBUG)

    ff = logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s")
    handler.setFormatter( ff )

    logger.addHandler( handler )

    # Full tracebacks should only go to the file
    traceback_log = logging.getLogger('CRAB3:traceback')
    traceback_log.propagate = False
    traceback_log.setLevel(logging.ERROR)
    traceback_log.addHandler(handler)


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
    addFileLogger( logger, workingpath = fullpath )

    return fullpath, requestName


def createCache(requestarea, server, uniquerequestname):
    touchfile = open(os.path.join(requestarea, '.requestcache'), 'w')
    neededhandlers = {
                      "Server" : server['conn'].host,
                      "Port" : server['conn'].port,
                      "RequestName" : uniquerequestname
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

    addFileLogger( logger, workingpath = requestarea )
    return cPickle.load(loadfile)

def initProxy(serverDN, myProxy, voRole, voGroup, delegate, logger):
    proxy = CredentialInteractions(
                                    serverDN,
                                    myProxy,
                                    voRole,
                                    voGroup,
                                    logger
                                  )

    logger.info("Checking credentials")
    userdn = proxy.createNewVomsProxy( timeleftthreshold = 600 )

    if delegate:
        logger.info("Registering user credentials")
        proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * 3)

    return userdn, proxy

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

def validURL(serverurl, attrtohave = ['scheme', 'netloc', 'hostname', 'port'], attrtonothave = ['path', 'params', 'query', 'fragment', 'username', 'password']):
    """
    returning false if the format is different from http://host:port
    """
    tempurl = serverurl
    if not serverurl.startswith('http://'):
        tempurl = 'http://' + serverurl
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
