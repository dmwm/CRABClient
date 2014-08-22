#!/usr/bin/env python
# encoding: utf-8
"""
This contains some utility methods for the client
"""

import os
import re
import datetime
import logging
import logging.handlers

import pkgutil
import sys
import cPickle
import logging

from string import upper
from optparse import OptionValueError
import CRABClient.Emulator
from CRABClient.client_exceptions import TaskNotFoundException, CachefileNotFoundException, ConfigurationException ,ConfigException


class colors:

    colordict = {
                'RED':'\033[91m',
                'GREEN':'\033[92m',
                'BLUE':'\033[93m',
                'GRAY':'\033[90m',
                'NORMAL':'\033[0m',
                'BOLD':'\033[1m' }

    RED    = colordict['RED']
    GREEN  = colordict['GREEN']
    BLUE   = colordict['BLUE']
    GRAY   = colordict['GRAY']
    NORMAL = colordict['NORMAL']
    BOLD   = colordict['BOLD']

class logfilter(logging.Filter):

    def filter(self, record):
        def removecolor(text):

            for color in colors.colordict.keys():
                if colors.colordict[color] in text:
                    text = text.replace(colors.colordict[color],'')

            return text

        # find color in record.msg

        if isinstance(record.msg, Exception):
            record.msg = removecolor(str(record.msg))
        else:
            record.msg = removecolor(record.msg)
        return True


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

    if requestName is None or not isinstance(requestName,str) or len(requestName) == 0:
        return prefix + postfix
    elif '/' in requestName:
        msg = '%sError %s: The "/" character is not accepted in the requestName parameter.\
         If your intention was to specity the location of your task, please use the config.General.workArea parameter' \
              % (colors.RED, colors.NORMAL)
        raise ConfigurationException(msg)
    else:
        return prefix + requestName # + '_' + postfix


def changeFileLogger(logger, workingpath = os.getcwd(), logname = 'crab.log'):
    """
    change file logger destination
    """
    logfullpath = os.path.join( workingpath, logname )
    logging.getLogger('CRAB3.all').logfile = logfullpath

    return logfullpath

def createWorkArea(logger, workingArea = '.', requestName = ''):
    """
    _createWorkArea_

    creates the working directory with the needed sub-folders
    in case it already exists it raises an exception
    """

    if workingArea is None or workingArea == '.' :
        workingArea = os.getenv('CRAB_WORKING_AREA', '.')
    elif not os.path.isabs(workingArea):
        workingArea = os.path.abspath(workingArea)

    ## create the working area if it is not there
    if not os.path.exists(workingArea):
        os.makedirs(workingArea)

    requestName = getRequestName(requestName)

    fullpath = os.path.join(workingArea, requestName)

    ## checking if there is no a duplicate
    if os.path.exists(fullpath):
        raise ConfigException("Working area '%s' already exists \nPlease change the requestName in the config file" % fullpath)

    ## creating the work area
    os.mkdir(fullpath)
    os.mkdir(os.path.join(fullpath, 'results'))
    os.mkdir(os.path.join(fullpath, 'inputs'))

    ## define the log file
    logfile = changeFileLogger( logger, workingpath = fullpath )

    return fullpath, requestName, logfile


def createCache(requestarea, host, port, uniquerequestname, voRole, voGroup, instance, originalConfig={}):
    touchfile = open(os.path.join(requestarea, '.requestcache'), 'w')
    neededhandlers = {
                      "Server" : host,
                      "Port" : port,
                      "RequestName" : uniquerequestname,
                      "voRole" : voRole,
                      "voGroup" : voGroup,
                      "instance" : instance,
                      "OriginalConfig" : originalConfig
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

    logfile = changeFileLogger( logger, workingpath = requestarea )
    return cPickle.load(loadfile), logfile


def getUserName(voRole, voGroup, logger):
    _, _, proxy = initProxy(voRole, voGroup, logger)
    return proxy.getUserName()


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
    if re.match('^\d+((?!(-\d+-))(\,|\-)\d+)*$',jobids):
        jobid=[]
        element=jobids.split(',')
        for number in element:
            if '-' in number:
                sub=number.split('-')
                jobid.extend(range(int(sub[0]),int(sub[1])+1))
            else:
                jobid.append(int(number))
        #removing duplicate and sort the list
        jobid = list(set(jobid))
        return [('jobids',job) for job in jobid]
    else:
        raise ConfigurationException("The command line option jobids should be a comma separated list of integers or range, no whitespace")


#XXX Trying to do it as a Command causes a lot of headaches (and workaround code).
#Since server_info class needs SubCommand, and SubCommand needs server_info for
#delegating the proxy then we are screwed
#If anyone has a better solution please go on, otherwise live with that one :) :)
from CRABClient.client_exceptions import RESTCommunicationException
from CRABClient import __version__

def server_info(subresource, server, proxyfilename, baseurl, **kwargs):
    """
    Get relevant information about the server
    """
    server = CRABClient.Emulator.getEmulator('rest')(server, proxyfilename, proxyfilename, version=__version__)
    requestdict= {'subresource' : subresource}
    requestdict.update(**kwargs)
    dictresult, status, reason = server.get(baseurl, requestdict)

    return dictresult['result'][0]
