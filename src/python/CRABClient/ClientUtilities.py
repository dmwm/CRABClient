"""
This module contains some utility methods for the client.
"""
from __future__ import print_function

import os
import re
import copy
import datetime
import logging.handlers
import time
import pkgutil
import sys
import cPickle
import subprocess
import traceback
from string import upper
from urlparse import urlparse
from optparse import OptionValueError

## CRAB dependencies
import CRABClient.Emulator
from CRABClient.ClientExceptions import ClientException, TaskNotFoundException, CachefileNotFoundException, ConfigurationException, ConfigException, UsernameException, ProxyException, RESTCommunicationException


DBSURLS = {'reader': {'global': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                      'phys01': 'https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader',
                      'phys02': 'https://cmsweb.cern.ch/dbs/prod/phys02/DBSReader',
                      'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'},
           'writer': {'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'}}


BASEURL = '/crabserver/'
SERVICE_INSTANCES = {'prod': 'cmsweb.cern.ch',
                     'preprod': 'cmsweb-testbed.cern.ch',
                     'dev': 'cmsweb-dev.cern.ch'}
BOOTSTRAP_ENVFILE = 'crab3env.json'
BOOTSTRAP_INFOFILE = 'crab3info.json'
BOOTSTRAP_CFGFILE = 'PSet.py'
BOOTSTRAP_CFGFILE_PKL = 'PSet.pkl'


class colors:
    colordict = {
                'RED':'\033[91m',
                'GREEN':'\033[92m',
                'BLUE':'\033[93m',
                'GRAY':'\033[90m',
                'NORMAL':'\033[0m',
                'BOLD':'\033[1m' }
    if sys.stdout.isatty():
        RED    = colordict['RED']
        GREEN  = colordict['GREEN']
        BLUE   = colordict['BLUE']
        GRAY   = colordict['GRAY']
        NORMAL = colordict['NORMAL']
        BOLD   = colordict['BOLD']
    else:
        RED, GREEN, BLUE, GRAY, NORMAL, BOLD = '', '', '', '', '', ''


class StopExecution():
    """
    Raise it to stop a client command execution without an error.
    """


## Dictionary with the client loggers.
LOGGERS = {}

## The log level for the console handler. Can be overwritten with setConsoleLogLevelVar().
CONSOLE_LOGLEVEL = logging.INFO

## Log level to mute a logger/handler.
LOGLEVEL_MUTE = logging.CRITICAL + 10


class logfilter(logging.Filter):
    def filter(self, record):
        def removecolor(text):
            if not text:
                return text
            for dummyColor, colorval in colors.colordict.iteritems():
                if colorval in text:
                    text = text.replace(colorval, '')
            return text
        # find color in record.msg
        if isinstance(record.msg, Exception):
            record.msg = removecolor(str(record.msg))
        else:
            record.msg = removecolor(record.msg)
        return True


def initLoggers():
    """
    Logging is using the hierarchy system: the CRAB3.all logger is a child of the
    CRAB3 logger. So everything that is logged to CRAB3.all will also go to CRAB3,
    but not viceversa. The CRAB3 logger uses a memory handler, which then will be
    flushed to a file handler in the 'finally' stage. So:
    CRAB3.all -> screen + file
    CRAB3     -> file
    """
    global LOGGERS
    ## The CRAB3 logger to memory/file.
    ## Start by setting up a (temporary) memory handler. The flush level is set to
    ## LOGLEVEL_MUTE so that any reasonable logging level should not cause any
    ## sudden flush. The reason for using a memory handler and flush later is that
    ## the log file is only known when the project directory is discovered. So we
    ## want to keep all logging in memory and flush to the log file later when the
    ## file is known. Include a filter in the handler to filter out color codes.
    tblogger = logging.getLogger('CRAB3')
    tblogger.setLevel(logging.DEBUG)
    memhandler = logging.handlers.MemoryHandler(capacity = 1024*10, flushLevel = LOGLEVEL_MUTE)
    memhandler.setFormatter(logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s"))
    memhandler.setLevel(logging.DEBUG)
    memhandler.addFilter(logfilter())
    tblogger.addHandler(memhandler)
    LOGGERS['CRAB3'] = tblogger

    ## Logger to the console. This is the logger that all the client code should
    ## use. Since it is a child of the CRAB3 logger, all log records created by this
    ## logger will propagate up to the CRAB3 logger handlers.
    logger = logging.getLogger('CRAB3.all')
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter('%(message)s'))
        console.setLevel(CONSOLE_LOGLEVEL)
        logger.addHandler(console)
    ## The log file name, although technically related to the CRAB3 logger, is kept
    ## in this logger, because this is the logger available at all time to all the
    ## client code.
    logger.logfile = os.path.join(os.getcwd(), 'crab.log')
    LOGGERS['CRAB3.all'] = logger

    return tblogger, logger, memhandler


def getLoggers(lvl):
    msg  = "%sError%s: The function getLoggers(loglevel) from CRABClient.ClientUtilities has been deprecated." % (colors.RED, colors.NORMAL)
    msg += " Please use the new function setConsoleLogLevel(loglevel) from CRABClient.UserUtilities instead."
    raise ClientException(msg)


def setConsoleLogLevelVar(lvl):
    global CONSOLE_LOGLEVEL
    CONSOLE_LOGLEVEL = lvl


def changeFileLogger(logger, workingpath = os.getcwd(), logname = 'crab.log'):
    """
    change file logger destination
    """
    logfullpath = os.path.join(workingpath, logname)
    logger.logfile = logfullpath
    return logfullpath


def flushMemoryLogger(logger, memhandler, logfilename):
    filehandler = logging.FileHandler(logfilename)
    ff = logging.Formatter("%(levelname)s %(asctime)s: \t %(message)s")
    filehandler.setFormatter(ff)
    filehandler.setLevel(logging.DEBUG)
    filehandler.addFilter(logfilter())
    logger.addHandler(filehandler)
    memhandler.setTarget(filehandler)
    memhandler.close()
    logger.removeHandler(memhandler)


def removeLoggerHandlers(logger):
    for h in copy.copy(logger.handlers):
        logger.removeHandler(h)


def getUrl(instance='prod', resource='workflow'):
    """
    Retrieve the url depending on the resource we are accessing and the instance.
    """
    if instance in SERVICE_INSTANCES.keys():
        return BASEURL + instance + '/' + resource
    elif instance == 'private':
        return BASEURL + 'dev' + '/' + resource
    raise ConfigurationException('Error: only the following instances can be used: %s' %str(SERVICE_INSTANCES.keys()))


def uploadlogfile(logger, proxyfilename, logfilename = None, logpath = None, instance = 'prod', serverurl = None, username = None):
    ## WMCore dependencies. Moved here to minimize dependencies in the bootstrap script
    from WMCore.Services.UserFileCache.UserFileCache import UserFileCache

    doupload = True

    if logfilename == None:
        logfilename = str(time.strftime("%Y-%m-%d_%H%M%S"))+'_crab.log'

    logger.info('Fetching user enviroment to log file')

    try:
        cmd = 'env'
        logger.debug('Running env command')
        pipe = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        stdout, dummyStderr = pipe.communicate()
        logger.debug('\n\n\nUSER ENVIROMENT\n%s' % stdout)
    except Exception as se:
        logger.debug('Failed to get the user env\nException message: %s' % (se))

    if logpath != None:
        if not os.path.exists(logpath):
            doupload = False
            logger.debug('%sError%s: %s does not exist' %(colors.RED, colors.NORMAL, logpath))
    else:
        if os.path.exists(str(os.getcwd()) + '/crab.log'):
            logpath = str(os.getcwd())+'/crab.log'
        else:
            logger.debug('%sError%s: Failed to find crab.log in current directory %s' % (colors.RED, colors.NORMAL, str(os.getcwd())))

    if serverurl == None and instance in SERVICE_INSTANCES.keys():
        serverurl = SERVICE_INSTANCES[instance]
    elif not instance in SERVICE_INSTANCES.keys() and serverurl != None:
        instance = 'private'
    elif not instance in SERVICE_INSTANCES.keys() and serverurl == None:
        logger.debug('%sError%s: serverurl is None' % (colors.RED, colors.NORMAL))
        doupload = False

    if proxyfilename == None:
        logger.debug('No proxy was given')
        doupload = False

    baseurl = getUrl(instance = instance , resource = 'info')
    if doupload:
        cacheurl = server_info('backendurls', serverurl, proxyfilename, baseurl)
        cacheurl = cacheurl['cacheSSL']
        cacheurldict = {'endpoint': cacheurl, "pycurl": True}

        ufc = UserFileCache(cacheurldict)
        logger.debug("cacheURL: %s\nLog file name: %s" % (cacheurl, logfilename))
        logger.info("Uploading log file...")
        ufc.uploadLog(logpath, logfilename)
        logger.info("%sSuccess%s: Log file uploaded successfully." % (colors.GREEN, colors.NORMAL))
        logfileurl = cacheurl + '/logfile?name='+str(logfilename)
        if not username:
            username = getUsernameFromSiteDB_wrapped(logger, quiet = True)
        if username:
            logfileurl += '&username='+str(username)
        logger.info("Log file URL: %s" % (logfileurl))
        return  logfileurl
    else:
        logger.info('Failed to upload the log file')
        logfileurl = False

    return  logfileurl


def getPlugins(namespace, plugins, skip):
    """
    _getPlugins_

    returns a dictionary with key='class name' and value='hook to the module'
    as input needs the package name that contains the different modules

    TODO: If we use WMCore more, replace with the WMFactory.
    """
    packagemod = __import__('%s.%s' % (namespace, plugins), globals(), locals(), plugins)
    fullpath   = packagemod.__path__[0]
    modules = {}
    ## iterating on the modules contained in that package
    for el in list(pkgutil.iter_modules([fullpath])):
        if el[1] not in skip:
            ## import the package module
            mod = __import__('%s.%s.%s' % (namespace, plugins, el[1]), globals(), locals(), el[1])
            ## add to the module dictionary.
            ## N.B. Utilitiy modules like LumiMask do not have classes inside them
            modules[el[1]] = getattr(mod, el[1], None)
            #set the default name if it has not been overridden in the class
            if not hasattr(modules[el[1]], 'name') and modules[el[1]]:
                setattr(modules[el[1]], 'name', modules[el[1]].__name__)

    return modules


def addPlugin(pluginpathname):
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
    _getAvailCommands_

    wrap the dynamic plug-in import for the available commands
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

    if requestName is None or not isinstance(requestName, str) or len(requestName) == 0:
        return prefix + postfix
    elif '/' in requestName:
        msg  = "%sError%s: The '/' character is not accepted in the requestName parameter." % (colors.RED, colors.NORMAL)
        msg += " If your intention was to specify the location of your task, please use the General.workArea parameter."
        raise ConfigurationException(msg)
    else:
        return prefix + requestName # + '_' + postfix


def createWorkArea(logger, workingArea = '.', requestName = ''):
    """
    _createWorkArea_

    creates the working directory with the needed sub-folders
    in case it already exists it raises an exception
    """

    if workingArea is None or workingArea == '.' :
        workingArea = os.getenv('CRAB_WORKING_AREA', '.')
    if not os.path.isabs(workingArea):
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
    logfile = changeFileLogger(logger, workingpath = fullpath)

    return fullpath, requestName, logfile


def createCache(requestarea, host, port, uniquerequestname, voRole, voGroup, instance, originalConfig={}):
    touchfile = open(os.path.join(requestarea, '.requestcache'), 'w')
    neededhandlers = {
                      "Server" : host,
                      "Port" : port,
                      "RequestName" : uniquerequestname,
                      "voRole" : voRole if voRole != '' else 'NULL',
                      "voGroup" : voGroup,
                      "instance" : instance,
                      "OriginalConfig" : originalConfig
                     }
    cPickle.dump(neededhandlers, touchfile)
    touchfile.close()


def getWorkArea(projdir):
    requestarea = ''
    requestname = ''
    if os.path.isabs(projdir):
        requestarea = projdir
        requestname = os.path.split(os.path.normpath(requestarea))[1]
    else:
        requestarea = os.path.abspath(projdir)
        requestname = projdir
    return requestarea, requestname


def loadCache(mydir, logger):
    requestarea, dummyRequestname = getWorkArea(mydir)
    cachename = os.path.join(requestarea, '.requestcache')
    #Check if the directory exists.
    if not os.path.isdir(requestarea):
        msg = "%s is not a valid CRAB project directory." % (requestarea)
        raise TaskNotFoundException(msg)
    #If the .requestcache file exists open it!
    if os.path.isfile(cachename):
        loadfile = open(cachename, 'r')
    else:
        msg = "Cannot find .requestcache file in CRAB project directory %s" % (requestarea)
        raise CachefileNotFoundException(msg)
    logfile = changeFileLogger(logger, workingpath = requestarea)
    return cPickle.load(loadfile), logfile


def getUserDN():
    """
    Retrieve the user DN from the proxy.
    """
    scram_cmd = 'which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`'
    ## Check if there is a proxy.
    cmd = scram_cmd + "; voms-proxy-info"
    process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Unable to retrieve DN from proxy:"
        msg += "\nError executing command: %s" % (cmd)
        msg += "\n  Stdout:\n    %s" % (str(stdout).replace('\n', '\n    '))
        msg += "\n  Stderr:\n    %s" % (str(stderr).replace('\n', '\n    '))
        raise ProxyException(msg)
    ## Retrieve DN from proxy.
    cmd = scram_cmd + "; voms-proxy-info -identity"
    process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    stdout, stderr = process.communicate()
    if process.returncode or not stdout:
        msg  = "Unable to retrieve DN from proxy:"
        msg += "\nError executing command: %s" % (cmd)
        msg += "\n  Stdout:\n    %s" % (str(stdout).replace('\n', '\n    '))
        msg += "\n  Stderr:\n    %s" % (str(stderr).replace('\n', '\n    '))
        raise ProxyException(msg)
    userdn = str(stdout.replace('\n', ''))
    return userdn


def getUserDN_wrapped(logger):
    """
    Wrapper function for getUserDN,
    catching exceptions and printing messages.
    """
    userdn = None
    logger.info('Retrieving DN from proxy...')
    try:
        userdn = getUserDN()
    except ProxyException as ex:
        msg = "%sError%s: %s" % (colors.RED, colors.NORMAL, ex)
        logger.error(msg)
    except Exception:
        msg  = "%Error%s: Failed to retrieve DN from proxy." % (colors.RED, colors.NORMAL)
        msg += "\n%s" % (traceback.format_exc())
        logger.error(msg)
    else:
        logger.info('DN is: %s' % (userdn))
    return userdn


def getUsernameFromSiteDB_wrapped(logger, quiet = False):
    """
    Wrapper function for getUsernameFromSiteDB,
    catching exceptions and printing messages.
    """
    from CRABClient.UserUtilities import getUsernameFromSiteDB
    username = None
    msg = "Retrieving username from SiteDB..."
    if quiet:
        logger.debug(msg)
    else:
        logger.info(msg)
    try:
        username = getUsernameFromSiteDB()
    except ProxyException as ex:
        msg = "%sError%s: %s" % (colors.RED, colors.NORMAL, ex)
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    except UsernameException as ex:
        msg = "%sError%s: %s" % (colors.RED, colors.NORMAL, ex)
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    except Exception:
        msg  = "%sError%s: Failed to retrieve username from SiteDB." % (colors.RED, colors.NORMAL)
        msg += "\n%s" % (traceback.format_exc()) 
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    else:
        msg = "Username is: %s" % (username)
        if quiet:
            logger.debug(msg)
        else:
            logger.info(msg)
    return username


def getUserDNandUsernameFromSiteDB(logger):
    userdn = getUserDN_wrapped(logger)
    username = getUsernameFromSiteDB_wrapped(logger) if userdn else None
    return {'DN': userdn, 'username': username}


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
    if re.match('^\d+((?!(-\d+-))(\,|\-)\d+)*$', jobids):
        jobid = []
        element = jobids.split(',')
        for number in element:
            if '-' in number:
                sub = number.split('-')
                jobid.extend(range(int(sub[0]), int(sub[1])+1))
            else:
                jobid.append(int(number))
        #removing duplicate and sort the list
        jobid = list(set(jobid))
        return [('jobids', job) for job in jobid]
    else:
        msg  = "The command line option --jobids takes a comma separated list of"
        msg += " integers or ranges, without whitespaces."
        raise ConfigurationException(msg)


def bootstrapDone():
    return 'CRAB3_BOOTSTRAP_DIR' in os.environ and os.environ['CRAB3_BOOTSTRAP_DIR']


def setSubmitParserOptions(parser):
    """ Set the option for the parser of the submit command.
        Method put here in the utilities since it is shared between the submit command and the crab3bootstrap script.
    """
    parser.add_option('-c', '--config',
                           dest = 'config',
                           default = None,
                           help = "CRAB configuration file.",
                           metavar = 'FILE')

    parser.add_option('--wait',
                           dest = 'wait',
                           default = False,
                           action = 'store_true',
                           help = "Continuously check the task status after submission.")

    parser.add_option('--dryrun',
                           dest = 'dryrun',
                           default = False,
                           action = 'store_true',
                           help = "Do not actually submit the task; instead, return how many jobs this task would create, "\
                                  "along with processing time and memory consumption estimates.")

    parser.add_option('--skip-estimates',
                           dest = 'skipEstimates',
                           default = False,
                           action = 'store_true',
                           help = "When executing a dry run, skip the processing time and memory consumption estimates.")

def validateSubmitOptions(options, args):
    """ If no configuration file was passed as an option, try to extract it from the first argument.
        Assume that the arguments can only be:
            1) the configuration file name (in the first argument), and
            2) parameters to override in the configuration file.
        The last ones should all contain an '=' sign, while the configuration file name should not.
        Also, the configuration file name should end with '.py'.
        If the first argument is not a python file name, use the default name 'crabConfig.py'.
    """

    if options.config is None:
        if len(args) and '=' not in args[0] and args[0][-3:] == '.py':
            options.config = args[0]
            del args[0]
        else:
            options.config = 'crabConfig.py'


#XXX Trying to do it as a Command causes a lot of headaches (and workaround code).
#Since server_info class needs SubCommand, and SubCommand needs server_info for
#delegating the proxy then we are screwed
#If anyone has a better solution please go on, otherwise live with that one :) :)
from CRABClient import __version__

def server_info(subresource, server, proxyfilename, baseurl, **kwargs):
    """
    Get relevant information about the server
    """
    server = CRABClient.Emulator.getEmulator('rest')(server, proxyfilename, proxyfilename, version=__version__)
    requestdict = {'subresource': subresource}
    requestdict.update(**kwargs)
    dictresult, dummyStatus, dummyReason = server.get(baseurl, requestdict)

    return dictresult['result'][0]


def cmd_exist(cmd):
    try:
        null = open("/dev/null", "w")
        subprocess.Popen(cmd, stdout=null, stderr=null)
        null.close()
        return True
    except OSError:
        return False

def checkStatusLoop(logger, server, uri, uniquerequestname, targetstatus, cmdname):
    logger.info("Waiting for task to be processed")

    maxwaittime = 900 #in second, changed to 15 minute max wait time, the original 1 hour is too long
    starttime = currenttime = time.time()
    endtime = currenttime + maxwaittime

    startimestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime))
    endtimestring  = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))

    logger.debug("Start time:%s" % (startimestring))
    logger.debug("Max wait time: %s s until : %s" % (maxwaittime,endtimestring))

    #logger.debug('Looking up detailed status of task %s' % uniquerequestname)

    continuecheck = True
    tmpresult = None
    logger.info("Checking task status")

    while continuecheck:
        currenttime = time.time()
        querytimestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(currenttime))

        logger.debug("Looking up detailed status of task %s" % (uniquerequestname))

        dictresult, status, reason = server.get(uri, data = {'workflow' : uniquerequestname})
        dictresult = dictresult['result'][0]

        if status != 200:
            msg  = "Error when trying to check the task status."
            msg += " Please check the task status later using 'crab status'."
            logger.error(msg)
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(uniquerequestname), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        logger.debug("Query Time: %s Task status: %s" % (querytimestring, dictresult['status']))

        logger.info("Task status: %s" % (dictresult['status']))
        if dictresult['status'] != tmpresult:
            tmpresult = dictresult['status']
            if dictresult['status'] in ['SUBMITFAILED', 'RESUBMITFAILED']:
                continuecheck = False
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " The %s of your task has failed." % ("resubmission" if cmdname == "resubmit" else "submission")
                logger.error(msg)
                if dictresult['taskFailureMsg']:
                    msg  = "%sFailure message%s:" % (colors.RED, colors.NORMAL)
                    msg += "\t%s" % (dictresult['taskFailureMsg'].replace('\n', '\n\t\t\t'))
                    logger.error(msg)
            elif dictresult['status'] in ['SUBMITTED', 'UPLOADED', 'UNKNOWN']: #until the node_state file is available status is unknown
                continuecheck = False
            else:
                logger.info("Please wait...")
                time.sleep(30)
        elif dictresult['status'] in ['NEW', 'HOLDING', 'QUEUED', 'RESUBMIT']:
            logger.info("Please wait...")
            time.sleep(30)
        else:
            continuecheck = False
            logger.info("Please check crab.log")
            logger.debug("Task status other than SUBMITFAILED, RESUBMITFAILED, SUBMITTED, UPLOADED, NEW, HOLDING, QUEUED, RESUBMIT")
        ## Break the loop if we were waiting already too much.
        if currenttime > endtime:
            continuecheck = False
            msg  = "Maximum query time exceeded."
            msg += " Please check the status of the %s later using 'crab status'." % ("resubmission" if cmdname == "resubmit" else "submission")
            logger.info(msg)
            waittime = currenttime - starttime
            logger.debug("Wait time: %s" % (waittime))

    if targetstatus == 'SUBMITTED':
        if tmpresult == 'SUBMITTED':
            msg  = "%sSuccess%s:" % (colors.GREEN, colors.NORMAL)
            msg += " Your task has been processed and your jobs have been %s successfully." % ("resubmitted" if cmdname == "resubmit" else "submitted")
            logger.info(msg)
        elif currenttime < endtime and tmpresult not in ['SUBMITFAILED', 'RESUBMITFAILED']:
            msg  = "The CRAB3 server finished processing your task."
            msg += " Use 'crab status' to see if your jobs have been %s successfully." % ("resubmitted" if cmdname == "resubmit" else "submitted")
            logger.info(msg)

    print('\a') #Generate audio bell
    logger.debug("Ended %s process." % ("resubmission" if cmdname == "resubmit" else "submission"))

