"""
This module contains some utility methods for the client.
"""
from __future__ import print_function

import os
import re
import copy
import datetime
import logging
import logging.handlers
import time
from time import gmtime
import pkgutil
import sys
import pickle
import subprocess
import traceback
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urlparse import urlparse
from optparse import OptionValueError

## CRAB dependencies
import CRABClient.Emulator
from ServerUtilities import uploadToS3, getDownloadUrlFromS3
from CRABClient.ClientExceptions import ClientException, TaskNotFoundException, CachefileNotFoundException, ConfigurationException, ConfigException, UsernameException, ProxyException, RESTCommunicationException, RucioClientException

# pickle files need to be opeb in different mode in python2 or python3
if sys.version_info >= (3, 0):
    PKL_W_MODE = 'wb'
    PKL_R_MODE = 'rb'
else:
    PKL_W_MODE = 'w'
    PKL_R_MODE = 'r'

DBSURLS = {'reader': {'global': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                      'phys01': 'https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader',
                      'phys02': 'https://cmsweb.cern.ch/dbs/prod/phys02/DBSReader',
                      'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'},
           'writer': {'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'}}

BOOTSTRAP_ENVFILE = 'crab3env.json'
BOOTSTRAP_INFOFILE = 'crab3info.json'
BOOTSTRAP_CFGFILE = 'PSet.py'
BOOTSTRAP_CFGFILE_PKL = 'PSet.pkl'

# next two shoul move to CRABServer/ServerUtilities since will be needed in TW and scheduler as well
RUCIO_QUOTA_WARNING_GB = 10  # when available Rucio quota is less than this, warn users
RUCIO_QUOTA_MINIMUM_GB = 1  # when available Rucio quota is less thatn this, refuse submission

class colors:  # pylint: disable=no-init
    colordict = {
                'RED':'\033[91m',
                'GREEN':'\033[92m',
                'BLUE':'\033[93m',
                'GRAY':'\033[90m',
                'NORMAL':'\033[0m',
                'BOLD':'\033[1m'}
    if sys.stdout.isatty():
        RED = colordict['RED']
        GREEN = colordict['GREEN']
        BLUE = colordict['BLUE']
        GRAY = colordict['GRAY']
        NORMAL = colordict['NORMAL']
        BOLD = colordict['BOLD']
    else:
        RED, GREEN, BLUE, GRAY, NORMAL, BOLD = '', '', '', '', '', ''


class StopExecution(Exception):
    """
    Raise it to stop a client command execution without an error.
    """


## Dictionary with the client loggers.
LOGGERS = {}

## The log level for the console handler. Can be overwritten with setConsoleLogLevelVar().
CONSOLE_LOGLEVEL = logging.INFO

## Log level to mute a logger/handler.
LOGLEVEL_MUTE = logging.CRITICAL + 10

## Log format
LOGFORMAT = {'logfmt': "%(levelname)s %(asctime)s.%(msecs)03d UTC: \t %(message)s", 'datefmt': "%Y-%m-%d %H:%M:%S"}
LOGFORMATTER = logging.Formatter(LOGFORMAT['logfmt'], LOGFORMAT['datefmt'])
LOGFORMATTER.converter = gmtime

class logfilter(logging.Filter):
    def filter(self, record):
        def removecolor(text):
            if not text:
                return text
            for dummyColor, colorval in colors.colordict.items():
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
    global LOGGERS  # pylint: disable=global-statement
    ## The CRAB3 logger to memory/file.
    ## Start by setting up a (temporary) memory handler. The flush level is set to
    ## LOGLEVEL_MUTE so that any reasonable logging level should not cause any
    ## sudden flush. The reason for using a memory handler and flush later is that
    ## the log file is only known when the project directory is discovered. So we
    ## want to keep all logging in memory and flush to the log file later when the
    ## file is known. Include a filter in the handler to filter out color codes.
    tblogger = logging.getLogger('CRAB3')
    tblogger.setLevel(logging.DEBUG)
    memhandler = logging.handlers.MemoryHandler(capacity=1024*10, flushLevel=LOGLEVEL_MUTE)
    memhandler.setFormatter(LOGFORMATTER)
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


def getLoggers():
    msg = "%sError%s: The function getLoggers(loglevel) from CRABClient.ClientUtilities has been deprecated." % (colors.RED, colors.NORMAL)
    msg += " Please use the new function setConsoleLogLevel(loglevel) from CRABClient.UserUtilities instead."
    raise ClientException(msg)


def setConsoleLogLevelVar(lvl):
    global CONSOLE_LOGLEVEL  # pylint: disable=global-statement
    CONSOLE_LOGLEVEL = lvl


def changeFileLogger(logger, workingpath=os.getcwd(), logname='crab.log'):
    """
    change file logger destination
    """
    logfullpath = os.path.join(workingpath, logname)
    logger.logfile = logfullpath
    return logfullpath


def flushMemoryLogger(logger, memhandler, logfilename):
    filehandler = logging.FileHandler(logfilename)
    filehandler.setFormatter(LOGFORMATTER)
    filehandler.setLevel(logging.DEBUG)
    filehandler.addFilter(logfilter())
    logger.addHandler(filehandler)
    memhandler.setTarget(filehandler)
    memhandler.close()
    logger.removeHandler(memhandler)


def removeLoggerHandlers(logger):
    for h in copy.copy(logger.handlers):
        logger.removeHandler(h)


def getColumn(dictresult, columnName):
    columnIndex = dictresult['desc']['columns'].index(columnName)
    value = dictresult['result'][columnIndex]
    if value == 'None':
        return None
    else:
        return value

def uploadlogfile(logger, proxyfilename, taskname=None, logfilename=None, logpath=None, instance=None, serverurl=None, username=None):

    doupload = True

    if logfilename == None:
        logfilename = str(time.strftime("%Y-%m-%d_%H%M%S"))+'_crab.log'

    logger.info('Fetching user enviroment to log file')

    try:
        logger.debug('Running env command')
        stdout, _, _ = execute_command(command='env')
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

    if proxyfilename == None:
        logger.debug('No proxy was given')
        doupload = False

    if doupload:
        # uploadLog is executed directly from crab main script, does not inherit from SubCommand
        # so it needs its own REST server instantiation
        restClass = CRABClient.Emulator.getEmulator('rest')
        crabserver = restClass(hostname=serverurl, localcert=proxyfilename, localkey=proxyfilename,
                               retry=2, logger=logger, verbose=False)
        crabserver.setDbInstance(instance)
        cacheurl = server_info(crabserver=crabserver, subresource='backendurls')['cacheSSL']

        logger.info("Uploading log file...")
        objecttype = 'clientlog'
        uploadToS3(crabserver=crabserver, filepath=logpath, objecttype=objecttype, taskname=taskname, logger=logger)
        logfileurl = getDownloadUrlFromS3(crabserver=crabserver, objecttype=objecttype, taskname=taskname, logger=logger)

        logger.info("Log file URL: %s" % (logfileurl))
        logger.info("%sSuccess%s: Log file uploaded successfully." % (colors.GREEN, colors.NORMAL))

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
    fullpath = packagemod.__path__[0]
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


def getJobTypes(jobtypepath='CRABClient', jobtypename='JobType'):
    """
    _getJobTypes_

    wrap the dynamic plug-in import for the available job types

    TODO: this can also be a call to get a specific job type from the server
    """
    allplugins = getPlugins(jobtypepath, jobtypename, ['BasicJobType'])
    result = {}
    for k in allplugins:
        result[k.upper()] = allplugins[k]
    return result


def getAvailCommands(subcmdpath='CRABClient', subcmdname='Commands'):
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


def getRequestName(requestName=None):
    """
    _getRequestName_

    create the directory name
    """
    prefix = 'crab_'
    postfix = str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))

    if requestName is None or not isinstance(requestName, str) or len(requestName) == 0:
        return prefix + postfix
    elif '/' in requestName:
        msg = "%sError%s: The '/' character is not accepted in the requestName parameter." % (colors.RED, colors.NORMAL)
        msg += " If your intention was to specify the location of your task, please use the General.workArea parameter."
        raise ConfigurationException(msg)
    else:
        return prefix + requestName  # + '_' + postfix


def createWorkArea(logger, workingArea='.', requestName=''):
    """
    _createWorkArea_

    creates the working directory with the needed sub-folders
    in case it already exists it raises an exception
    """

    if workingArea is None or workingArea == '.':
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
    logfile = changeFileLogger(logger, workingpath=fullpath)

    return fullpath, requestName, logfile


def createCache(requestarea, host, port, uniquerequestname, voRole, voGroup, instance, originalConfig=None):
    originalConfig = originalConfig or {}
    touchfile = open(os.path.join(requestarea, '.requestcache'), PKL_W_MODE)
    neededhandlers = {
        "Server" : host,
        "Port" : port,
        "RequestName" : uniquerequestname,
        "voRole" : voRole if voRole != '' else 'NULL',
        "voGroup" : voGroup,
        "instance" : instance,
        "OriginalConfig" : originalConfig
    }
    pickle.dump(neededhandlers, touchfile, protocol=0)
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
        loadfile = open(cachename, PKL_R_MODE)
    else:
        msg = "Cannot find .requestcache file in CRAB project directory %s" % (requestarea)
        raise CachefileNotFoundException(msg)
    #TODO should catch ValueError: unsupported pickle protocol: 4 in following line and flag it
    # as using python3 environment on a task created in python2 env.
    logfile = changeFileLogger(logger, workingpath=requestarea)
    return pickle.load(loadfile), logfile


def getUserProxy():
    """
    Retrieve the user proxy filename
    """
    undoScram = 'which scram >/dev/null 2>&1 && eval `scram unsetenv -sh`'
    ## Check if there is a proxy.
    #cmd = undoScram + "; voms-proxy-info"
    #process = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
    #stdout, stderr = process.communicate()
    #if process.returncode or not stdout:
    #    msg  = "Unable to retrieve DN from proxy:"
    #    msg += "\nError executing command: %s" % (cmd)
    #    msg += "\n  Stdout:\n    %s" % (str(stdout).replace('\n', '\n    '))
    #    msg += "\n  Stderr:\n    %s" % (str(stderr).replace('\n', '\n    '))
    #    raise ProxyException(msg)
    ## Retrieve DN from proxy.
    cmd = undoScram + "; voms-proxy-info -path"
    stdout, stderr, returncode = execute_command(command=cmd)
    if returncode or not stdout:
        msg = "Unable to find proxy file:"
        msg += "\nError executing command: %s" % (cmd)
        msg += "\n  Stdout:\n    %s" % (str(stdout).replace('\n', '\n    '))
        msg += "\n  Stderr:\n    %s" % (str(stderr).replace('\n', '\n    '))
        raise ProxyException(msg)
    proxyFile = str(stdout.strip())
    return proxyFile

def getUsernameFromCRIC_wrapped(logger, proxyFileName=None, quiet=False):
    """
    Wrapper function for getUsernameFromCRIC,
    catching exceptions and printing messages.
    """
    from CRABClient.UserUtilities import getUsernameFromCRIC
    username = None
    msg = "Retrieving username from CRIC..."
    if quiet:
        logger.debug(msg)
    else:
        logger.info(msg)
    try:
        username = getUsernameFromCRIC(proxyFileName)
    except ProxyException as ex:
        msg = "%sError ProxyException%s: %s" % (colors.RED, colors.NORMAL, ex)
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    except UsernameException as ex:
        msg = "%sError UsernameException%s: %s" % (colors.RED, colors.NORMAL, ex)
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    except Exception:
        msg = "%sError GenericException%s: Failed to retrieve username from CRIC." % (colors.RED, colors.NORMAL)
        msg += "\n%s" % (traceback.format_exc())
        if quiet:
            logger.debug(msg)
        else:
            logger.error(msg)
    else:
        msg = "Username is: %s" % (username)
        if not quiet:
            logger.info(msg)
    return username

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


def validURL(serverurl, attrtohave=None, attrtonothave=None):
    """
    returning false if the format is different from https://host:port
    """
    attrtohave = attrtohave or ['scheme', 'netloc', 'hostname']
    attrtonothave = attrtonothave or ['path', 'params', 'query', 'fragment', 'username', 'password']
    tempurl = serverurl
    if not serverurl.startswith('https://'):
        tempurl = 'https://' + serverurl
    parsedurl = urlparse(tempurl)
    for elem in attrtohave:
        elemval = getattr(parsedurl, elem, None)
        if str(elemval) == '' or elemval is None:
            return False
    for elem in attrtonothave:
        elemval = getattr(parsedurl, elem, None)
        if not str(elemval) == '' and elemval is not None:
            return False
    return True


def compareJobids(a, b):
    """ Compare two job IDs.  Probe jobs (0-*) come first, then processing
        jobs (>1), then tail jobs (>1-*).
        Return value as expected from python2 cmp() builtin:
        cmp(x,y)  return value is negative if x < y, zero if x == y and strictly positive if x > y
    """
    aa = [int(x) for x in a.split('-')]
    bb = [int(x) for x in b.split('-')]
    if len(aa) < len(bb):
        if bb[0] == 0:
            return 1
        return -1
    elif len(aa) > len(bb):
        if aa[0] == 0:
            return -1
        return 1
    elif aa[0] == bb[0]:
        if aa[1] == bb[1]:
            return 0
        else:
            return 1 if aa[1] > bb[1] else -1
    return 1 if aa[0] > bb[0] else -1


def validateJobids(jobids, allowLists=True):
    #check the format of jobids
    if re.match('^\d+((?!(-\d+-))(\,|\-)\d+)*$', jobids):
        jobid = []
        element = jobids.split(',')
        for number in element:
            if '-' in number and allowLists:
                sub = number.split('-')
                jobid.extend(range(int(sub[0]), int(sub[1])+1))
            else:
                jobid.append(int(number) if allowLists else number)
        #removing duplicate and sort the list
        jobid = list(set(jobid))
        return [('jobids', job) for job in jobid]
    else:
        msg = "The command line option --jobids takes a comma separated list of"
        msg += " integers or ranges, without whitespaces."
        raise ConfigurationException(msg)


def bootstrapDone():
    return 'CRAB3_BOOTSTRAP_DIR' in os.environ and os.environ['CRAB3_BOOTSTRAP_DIR']


def setSubmitParserOptions(parser):
    """ Set the option for the parser of the submit command.
        Method put here in the utilities since it is shared between the submit command and the crab3bootstrap script.
    """
    parser.add_option('-c', '--config',
                           dest='config',
                           default=None,
                           help="CRAB configuration file.",
                           metavar='FILE')

    parser.add_option('--wait',
                           dest='wait',
                           default=False,
                           action='store_true',
                           help="DEPRECATED.")

    parser.add_option('--dryrun',
                           dest='dryrun',
                           default=False,
                           action='store_true',
                           help="Do not actually submit the task; instead, return how many jobs this task would create, "\
                                  "along with processing time and memory consumption estimates.")

    parser.add_option('--skip-estimates',
                           dest='skipEstimates',
                           default=False,
                           action='store_true',
                           help="When executing a dry run, skip the processing time and memory consumption estimates.")

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

def server_info(crabserver=None, subresource=None):
    """
    Get relevant information about the server
    """

    api = 'info'
    requestdict = {'subresource': subresource} if subresource else {}
    dictresult, dummyStatus, dummyReason = crabserver.get(api, requestdict)

    return dictresult['result'][0]


def cmd_exist(cmd):
    try:
        null = open("/dev/null", "w")
        subprocess.Popen(cmd, stdout=null, stderr=null)
        null.close()
        return True
    except OSError:
        return False

def checkStatusLoop(logger, server, api, taskname, targetstatus, cmdname):
    logger.info("Waiting for task to be processed")

    maxwaittime = 900 #in second, changed to 15 minute max wait time, the original 1 hour is too long
    starttime = currenttime = time.time()
    endtime = currenttime + maxwaittime

    startimestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime))
    endtimestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))

    logger.debug("Start time:%s" % (startimestring))
    logger.debug("Max wait time: %s s until : %s" % (maxwaittime, endtimestring))

    #logger.debug('Looking up detailed status of task %s' % taskname)

    continuecheck = True
    tmpresult = None
    logger.info("Checking task status")

    while continuecheck:
        currenttime = time.time()
        querytimestring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(currenttime))

        logger.debug("Looking up status of task %s" % (taskname))
        # use same call as in Commands/status.py
        crabDBInfo, status, reason = server.get(api='task', data={'subresource':'search', 'workflow':taskname})

        if status != 200:
            msg = "Error when trying to check the task status."
            msg += " Please check the task status later using 'crab status'."
            logger.error(msg)
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(taskname), str(crabDBInfo), str(reason))
            raise RESTCommunicationException(msg)

        taskStatus = getColumn(crabDBInfo, 'tm_task_status')
        logger.debug("Query Time: %s Task status: %s" % (querytimestring, taskStatus))
        logger.info("Task status: %s" % (taskStatus))
        if taskStatus != tmpresult:
            tmpresult = taskStatus
            if taskStatus in ['SUBMITFAILED', 'RESUBMITFAILED']:
                continuecheck = False
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " The %s of your task has failed." % ("resubmission" if cmdname == "resubmit" else "submission")
                logger.error(msg)
                taskFailureMsg = getColumn(crabDBInfo, 'tm_task_failure')
                if taskFailureMsg:
                    msg = "%sFailure message%s:" % (colors.RED, colors.NORMAL)
                    msg += "\t%s" % (taskFailureMsg.replace('\n', '\n\t\t\t'))
                    logger.error(msg)
            elif taskStatus in ['SUBMITTED', 'UPLOADED']:
                continuecheck = False
            else:
                logger.info("Please wait...")
                time.sleep(30)
        elif taskStatus in ['NEW', 'HOLDING', 'QUEUED', 'RESUBMIT']:
            logger.info("Please wait...")
            time.sleep(30)
        else:
            continuecheck = False
            logger.info("Please check crab.log")
            logger.debug("Task status other than SUBMITFAILED, RESUBMITFAILED, SUBMITTED, UPLOADED, NEW, HOLDING, QUEUED, RESUBMIT")
        ## Break the loop if we were waiting already too much.
        if currenttime > endtime:
            continuecheck = False
            msg = "Maximum query time exceeded."
            msg += " Please check the status of the %s later using 'crab status'." % ("resubmission" if cmdname == "resubmit" else "submission")
            logger.info(msg)
            waittime = currenttime - starttime
            logger.debug("Wait time: %s" % (waittime))

    if targetstatus == 'SUBMITTED':
        if tmpresult == 'SUBMITTED':
            msg = "%sSuccess%s:" % (colors.GREEN, colors.NORMAL)
            msg += " Your task has been processed and your jobs have been %s successfully." % ("resubmitted" if cmdname == "resubmit" else "submitted")
            logger.info(msg)
        elif currenttime < endtime and tmpresult not in ['SUBMITFAILED', 'RESUBMITFAILED']:
            msg = "The CRAB3 server finished processing your task."
            msg += " Use 'crab status' to see if your jobs have been %s successfully." % ("resubmitted" if cmdname == "resubmit" else "submitted")
            logger.info(msg)

    print('\a') #Generate audio bell

    logger.debug("Ended %s process." % ("resubmission" if cmdname == "resubmit" else "submission"))


def execute_command(command=None, logger=None, timeout=None, redirect=True):
    """
    execute command with optional logging and timeout (in seconds).
    NOTE: TIMEOUT ONLY WORKS IF command IS A ONE WORD COMMAND
    Returns a 3-ple: stdout, stderr, rc
      rc=0 means success.
      rc=124 (SIGTERM) means that command timed out
    Redirection of std* can be turned off if the command will need to interact with caller
    writing messages and/or asking for input, like if needs to get a passphrase to access
    usercert/key for (my)proxy creation as in
    https://github.com/dmwm/WMCore/blob/75c5abd83738a6a3534027369cd6e109667de74e/src/python/WMCore/Credential/Proxy.py#L383-L385
    Should eventually go in ServerUtilities and be used everywhere.
    Put temperarely in ClientUtilities to be able to test new client with current Server
    Imported here from WMCore/Credential/Proxy.py
    """

    stdout, stderr, rc = None, None, 99999
    if logger:
        logger.debug('Executing command :\n %s' % command)
    if timeout:
        if logger:
            logger.debug('add timeout at %s seconds', timeout)
        command = ('timeout %s ' % timeout ) + command
    if redirect:
        proc = subprocess.Popen(
            command, shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
    else:
        proc = subprocess.Popen(command, shell=True)

    out, err = proc.communicate()
    rc = proc.returncode
    if rc == 124 and timeout:
        if logger:
            logger.error('ERROR: Timeout after %s seconds in executing:\n %s' % (timeout,command))
    # for Py3 compatibility
    stdout = out.decode(encoding='UTF-8') if out else ''
    stderr = err.decode(encoding='UTF-8') if err else ''
    if logger:
        logger.debug('output : %s\n error: %s\n retcode : %s' % (stdout, stderr, rc))

    return stdout, stderr, rc
