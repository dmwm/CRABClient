#!/usr/bin/env python
# encoding: utf-8
"""
This contains some utility methods for the client
"""

import os
import datetime
import pkgutil
import sys
import cPickle


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
    return getPlugins(jobtypepath, ['BasicJobType'])


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
    _addineFileLogger
    """
    import logging
    logfullpath = os.path.join( workingpath, logname )
    logger.debug("Setting log file %s " % logfullpath)

    handler = logging.FileHandler( logfullpath )
    handler.setLevel(logging.DEBUG)

    ff = logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s")
    handler.setFormatter( ff )

    #logging.getLogger('CRAB').addHandler( handler )
    logger.addHandler( handler )


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
                      "Server:" : server['conn'].host,
                      "Port:" : server['conn'].port,
                      "RequestName" : uniquerequestname
                     }
    cPickle.dump(neededhandlers, touchfile)
    touchfile.close()

def loadCache(requestarea):
    loadfile = open(os.path.join(requestarea, '.requestcache'), 'r')
    return cPickle.load(loadfile) 
