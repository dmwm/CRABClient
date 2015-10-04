import os
import re
import imp 
import json
import types
from ast import literal_eval
from optparse import OptionParser

import CRABClient.Emulator
from CRABClient import SpellChecker
from CRABClient.__init__ import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.CRABOptParser import CRABCmdOptParser
from CRABClient.ClientUtilities import BASEURL, SERVICE_INSTANCES
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.ClientUtilities import loadCache, getWorkArea, server_info, createWorkArea
from CRABClient.ClientMapping import renamedParams, commandsConfiguration, configParametersInfo, getParamDefaultValue
from CRABClient.ClientExceptions import ConfigurationException, MissingOptionException, EnvironmentException

from WMCore.Credential.Proxy import Proxy
from WMCore.Configuration import loadConfigurationFile, Configuration

#if certificates in myproxy expires in less than RENEW_MYPROXY_THRESHOLD days renew them
RENEW_MYPROXY_THRESHOLD = 15

class ConfigCommand:
    """
    Commands which needs to load the configuration file (e.g.: submit, publish) must subclass ConfigCommand
    Provides methods for loading the configuration file handling the errors
    """

    def loadConfig(self, configname, overrideargs = None):
        """
        Load the configuration file
        """
        ## If the configuration is alredy an object it doesn't need to be loaded from the file.
        if isinstance(configname, Configuration):
            self.configuration = configname
            valid, configmsg = self.validateConfig()
            if not valid:
                configmsg += "\nThe documentation about the CRAB configuration file can be found in"
                configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
                raise ConfigurationException(configmsg)
            return

        if not os.path.isfile(configname):
            raise ConfigurationException("CRAB configuration file %s not found." % (configname))
        self.logger.info("Will use CRAB configuration file %s" % (configname))
        try:
            self.logger.debug("Loading CRAB configuration file.")
            self.configuration = loadConfigurationFile(os.path.abspath(configname))
            ## Overwrite configuration parameters passed as arguments in the command line.
            if overrideargs:
                for singlearg in overrideargs:
                    ## The next line is needed, because we allow the config to be passed as argument
                    ## instead via the --config option.
                    if singlearg == configname: continue
                    if len(singlearg.split('=',1)) == 1:
                        self.logger.info("Wrong format in command-line argument '%s'. Expected format is <section-name>.<parameter-name>=<parameter-value>." % (singlearg))
                        if len(singlearg) > 1 and singlearg[0] == '-':
                            self.logger.info("If the argument '%s' is an option to the %s command, try 'crab %s %s [value for %s option (if required)] [arguments]'." \
                                             % (singlearg, self.__class__.__name__, self.__class__.__name__, singlearg, singlearg))
                        raise ConfigurationException("ERROR: Wrong command-line format.")
                    fullparname, parval = singlearg.split('=',1)
                    # now supporting just one sub params, eg: Data.inputFiles, User.email, ...
                    parnames = fullparname.split('.', 1)
                    if len(parnames) == 1:
                        self.logger.info("Wrong format in command-line argument '%s'. Expected format is <section-name>.<parameter-name>=<parameter-value>." % (singlearg))
                        raise ConfigurationException("ERROR: Wrong command-line format.")
                    self.configuration.section_(parnames[0])
                    type = configParametersInfo.get(fullparname, {}).get('type', 'undefined')
                    if type in ['undefined', 'StringType']:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("\'%s\'" % parval))
                        self.logger.debug("Overriden parameter %s with '%s'" % (fullparname, parval))
                    else:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("%s" % parval))
                        self.logger.debug("Overriden parameter %s with %s" % (fullparname, parval))
            valid, configmsg = self.validateConfig() ## Subclasses of SubCommand overwrite this method if needed.
        except RuntimeError as re:
            configmsg  = "Syntax error in CRAB configuration file:\n%s" % (self._extractReason(configname, re))
            configmsg += "\nPlease refer to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3CommonErrors#Syntax_error_in_CRAB_configurati"
            configmsg += "\nSee the ./crab.log file for more details."
            configmsg += "\nThe documentation about the CRAB configuration file can be found in"
            configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
            raise ConfigurationException(configmsg)
        else:
            if not valid:
                configmsg += "\nThe documentation about the CRAB configuration file can be found in"
                configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
                raise ConfigurationException(configmsg)


    def _extractReason(self, configname, re):
        """
        To call in case of error loading the configuration file
        Get the reason of the failure without the stacktrace. Put the stacktrace in the crab.log file
        """
        #get only the error wihtout the stacktrace
        msg = str(re)
        filename = os.path.abspath( configname )
        cfgBaseName = os.path.basename( filename ).replace(".py", "")
        cfgDirName = os.path.dirname( filename )
        if  not cfgDirName:
            modPath = imp.find_module(cfgBaseName)
        else:
            modPath = imp.find_module(cfgBaseName, [cfgDirName])
        try:
            modRef = imp.load_module(cfgBaseName, modPath[0],
                                     modPath[1], modPath[2])
        except Exception as ex:
            msg = str(ex)

        return msg


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there.
        Not all the commands require a configuration.
        """
        ## Check that the configuration object has the sections we expect it to have.
        ## (WMCore already checks that attributes added to the configuration object are of type ConfigSection.)
        ## Even if not all configuration sections need to be there, we anyway request
        ## the user to add all the sections in the configuration file.
        if not hasattr(self.configuration, 'General'):
            msg = "Invalid CRAB configuration: Section 'General' is missing."
            return False, msg
        if not hasattr(self.configuration, 'JobType'):
            msg = "Invalid CRAB configuration: Section 'JobType' is missing."
            return False, msg
        if not hasattr(self.configuration, 'Data'):
            msg = "Invalid CRAB configuration: Section 'Data' is missing."
            return False, msg
        if not hasattr(self.configuration, 'Site'):
            msg = "Invalid CRAB configuration: Section 'Site' is missing."
            return False, msg

        ## Some parameters may have been renamed. Check here if the configuration file has an old
        ## parameter defined, and in that case tell the user what is the new parameter name.
        for old_param, new_param in renamedParams.iteritems():
            if len(old_param.split('.')) != 2 or len(new_param['newParam'].split('.')) != 2:
                continue
            old_param_section, old_param_name = old_param.split('.')
            if hasattr(self.configuration, old_param_section) and hasattr(getattr(self.configuration, old_param_section), old_param_name):
                msg = "Invalid CRAB configuration: Parameter %s has been renamed to %s" % (old_param, new_param['newParam'])
                if new_param['version'] != None:
                    msg += " starting from CRAB %s" % (new_param['version'])
                msg += "; please change your configuration file accordingly."
                return False, msg

        ## Check if there are unknown parameters (and try to suggest the correct parameter name).
        all_config_params = configParametersInfo.keys()
        SpellChecker.DICTIONARY = SpellChecker.train(all_config_params)
        for section in self.configuration.listSections_():
            for attr in getattr(self.configuration, section).listSections_():
                param = (section + '.' + attr)
                if not SpellChecker.is_correct(param):
                    msg  = "Invalid CRAB configuration: Parameter %s is not known." % (param)
                    if SpellChecker.correct(param) != param:
                        msg += " Maybe you mean %s?" % (SpellChecker.correct(param))
                    return False, msg

        ## Check that each parameter specified in the configuration file is of the
        ## type specified in the configuration map.
        ## Check that, if a parameter is a required one and it has no default value,
        ## then it must be specified in the configuration file.
        for paramName, paramInfo in configParametersInfo.iteritems():
            requiredTypeName = paramInfo['type']
            try:
                requiredType = getattr(types, requiredTypeName)
            except AttributeError as ex:
                msg = "Invalid type %s specified in CRABClient configuration mapping for parameter %s." % (requiredTypeName, paramName)
                return False, msg
            attrs = paramName.split('.')
            obj = self.configuration
            while attrs and obj is not None:
                obj = getattr(obj, attrs.pop(0), None)
            if obj is not None:
                if type(obj) != requiredType:
                    msg = "Invalid CRAB configuration: Parameter %s requires a value of type %s (while a value of type %s was given)." \
                          % (paramName, str(requiredType), str(type(obj)))
                    if paramName == "Data.userInputFiles":
                        msg += "\nIn CRAB v3.3.14 the configuration parameter Data.userInputFiles has been modified to directly take a (python) list of primary input files."
                        msg += " Previously it was taking the name of a local text file where the primary input files were listed."
                        msg += " One can still use a text file and convert its content into a python list by doing Data.userInputFiles = list(open('my_list_of_files.txt'))"
                    return False, msg
                elif requiredType == list:
                    if not all(isinstance(arg, str) for arg in obj):
                        msg = "Invalid CRAB configuration: Parameter %s has to be a list of strings." % (paramName)
                        return False, msg
            elif getParamDefaultValue(paramName) is None and paramInfo['required']:
                msg = "Invalid CRAB configuration: Parameter %s is missing." % (paramName)
                return False, msg

        return True, "Valid configuration"


class SubCommand(ConfigCommand):

    ####### These options can be overrhidden if needed ########
    ## setting visible = False doesn't allow the sub-command to be called from CLI
    visible = True
    proxyfilename = None
    shortnames = []
    usage = "usage: %prog [command-options] [args]"
    #Default command name is the name of the command class, but it is also possible to set the name attribute in the subclass
    #if the command name does not correspond to the class name (getlog => get-log)


    def getUrl(self, instance='prod', resource='workflow'):
        """
        Retrieve the url depending on the resource we are accessing and the instance.
        """
        if instance in SERVICE_INSTANCES.keys():
            return BASEURL + instance + '/' + resource
        elif instance == 'private':
            return BASEURL + 'dev' + '/' + resource
        raise ConfigurationException('Error: only %s instances can be used.' % str(SERVICE_INSTANCES.keys()))


    def __init__(self, logger, cmdargs = None, disable_interspersed_args = False):
        """
        Initialize common client parameters
        """
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__

        ## The command logger.
        self.logger = logger
        self.logfile = self.logger.logfile
        self.logger.debug("Executing command: '%s'" % str(self.name))

        self.proxy = None
        self.restClass = CRABClient.Emulator.getEmulator('rest')

        ## Get the command configuration.
        self.cmdconf = commandsConfiguration.get(self.name)

        ## Get the CRAB cache file.
        self.crab3dic = self.getConfiDict()

        ## The options parser.
        self.parser = CRABCmdOptParser(self.name, self.__doc__,  disable_interspersed_args)

        ## Define the command options.
        self.setSuperOptions()

        ## Parse the command options/arguments.
        cmdargs = cmdargs or []
        (self.options, self.args) = self.parser.parse_args(cmdargs)

        ## Validate the command options.
        self.validateOptions()

        ## Get the VO group/role from the command options (if the command requires these
        ## options).
        proxyOptsSetPlace = {'set_in': {'group': "default", 'role': "default"}, 'for_set_use': ""}
        msgadd = []
        self.voGroup, self.voRole = "", "NULL"
        if self.cmdconf['requiresProxyVOOptions']:
            proxyOptsSetPlace['for_set_use'] = "cmdopts"
            if self.options.voGroup is not None:
                self.voGroup = self.options.voGroup
                proxyOptsSetPlace['set_in']['group'] = "cmdopts"
                msgadd.append("VO group '%s'" % (self.voGroup))
            if self.options.voRole is not None:
                self.voRole = self.options.voRole if self.options.voRole != "" else "NULL"
                proxyOptsSetPlace['set_in']['role'] = "cmdopts"
                msgadd.append("VO role '%s'" % (self.voRole))
        if msgadd:
            msg = "Using %s as specified in the crab command options." % (" and ".join(msgadd))
            self.logger.debug(msg)

        ## Create the object that will do the proxy operations. We don't really care
        ## what VO role and group and server URL we pass to the constructor, because
        ## these are not used until we do the proxy delegation to the myproxy server.
        ## And this happens in handleProxy(), which is called after we load the
        ## configuration file and retrieve the final values for those parameters.
        ## handleProxy() takes care of passing those parameters to self.proxy.
        self.proxy = CredentialInteractions('', '', self.voRole, self.voGroup, self.logger, '')

        ## If the user didn't use the --proxy command line option, and if there isn't a
        ## valid proxy already, we create a new one with the current VO role and group
        ## (as commented above, we don't really care what are the VO role and group so
        ## far).
        self.proxyCreated = False
        if not self.options.proxy and self.cmdconf['initializeProxy']:
            self.proxyCreated = self.proxy.createNewVomsProxySimple(timeLeftThreshold = 720)

        ## If there is an input configuration file:
        if hasattr(self.options, 'config') and self.options.config is not None:
            proxyOptsSetPlace['for_set_use'] = "config"
            ## Load the configuration file and validate it.
            self.loadConfig(self.options.config, self.args)
            ## Create the CRAB project directory.
            self.requestarea, self.requestname, self.logfile = createWorkArea(self.logger, \
                                                                              getattr(self.configuration.General, 'workArea', None), \
                                                                              getattr(self.configuration.General, 'requestName', None))
            ## Get the VO group/role from the configuration file.
            msgadd = []
            if hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voGroup'):
                self.voGroup = self.configuration.User.voGroup
                proxyOptsSetPlace['set_in']['group'] = "config"
                msgadd.append("VO group '%s'" % (self.voGroup))
            if hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voRole'):
                self.voRole = self.configuration.User.voRole if self.configuration.User.voRole != "" else "NULL"
                proxyOptsSetPlace['set_in']['role'] = "config"
                msgadd.append("VO role '%s'" % (self.voRole))
            if msgadd:
                msg = "Using %s as specified in the CRAB configuration file." % (" and ".join(msgadd))
                self.logger.debug(msg)

        ## If an input project directory was given, load the request cache and take the
        ## server URL from it. If the VO group/role was not given in the command options,
        ## take it also from the request cache.
        if self.cmdconf['requiresDirOption']:
            self.loadLocalCache(proxyOptsSetPlace)

        ## If the server URL isn't already set, we check the args and then the config.
        if not hasattr(self, 'serverurl') and self.cmdconf['requiresREST']:
            self.instance, self.serverurl = self.serverInstance()
        elif not self.cmdconf['requiresREST']:
            self.instance, self.serverurl = None, None

        ## Update (or create) the CRAB cache file.
        self.updateCRABCacheFile()

        ## At this point there should be a valid proxy, because we have already checked that and
        ## eventually created a new one. If the proxy was not created by CRAB, we check that the
        ## VO role/group in the proxy are the same as specified by the user in the configuration
        ## file (or in the command line options). If it is not, we ask the user if he wants to 
        ## overwrite the current proxy. If he doesn't want to overwrite it, we don't continue 
        ## and ask him to provide the VO role/group as in the existing proxy. 
        ## Finally, delegate the proxy to myproxy server.
        self.handleProxy(proxyOptsSetPlace)

        ## Logging user command and options used for debuging purpose.
        self.logger.debug('Command use: %s' % self.name)
        self.logger.debug('Options use: %s' % cmdargs)
        if self.cmdconf['requiresREST']:
            self.checkversion(self.getUrl(self.instance, resource='info'))
            self.uri = self.getUrl(self.instance)
        self.logger.debug("Instance is %s" %(self.instance))
        self.logger.debug("Server base url is %s" %(self.serverurl))
        if self.cmdconf['requiresREST']:
            self.logger.debug("Command url %s" %(self.uri))


    def serverInstance(self):
        """
        Deriving the correct instance to use and the server url. Client is allowed to propagate the instance name and corresponding url
        via crabconfig.py or crab option --instance. The variable passed via crab option will always be used over the variable
        in crabconfig.py. Instance name other than specify in the SERVICE_INSTANCE will be treated as a private instance.
        """
        serverurl = None

        #Will be use to print available instances
        available_instances = ', '.join(SERVICE_INSTANCES)

        if hasattr(self.options, 'instance') and self.options.instance is not None:
            if hasattr(self, 'configuration') and hasattr(self.configuration.General, 'instance') and self.configuration.General.instance is not None:
                msg  = "%sWarning%s: CRAB configuration parameter General.instance is overwritten by the command option --instance;" % (colors.RED, colors.NORMAL)
                msg += " %s intance will be used." % (self.options.instance)
                self.logger.info(msg)
            if self.options.instance in SERVICE_INSTANCES.keys():
                instance = self.options.instance
                serverurl = SERVICE_INSTANCES[instance]
            else:
                instance = 'private'
                serverurl = self.options.instance
        elif hasattr(self, 'configuration') and hasattr(self.configuration.General, 'instance') and self.configuration.General.instance is not None:
            if self.configuration.General.instance in SERVICE_INSTANCES.keys():
                instance = self.configuration.General.instance
                serverurl = SERVICE_INSTANCES[instance]
            else:
                instance = 'private'
                serverurl = self.configuration.General.instance
        else:
            instance = getParamDefaultValue('General.instance')
            serverurl = SERVICE_INSTANCES[instance]

        return instance, serverurl


    def checkversion(self, baseurl = None):
        compatibleVersions = server_info('version', self.serverurl, self.proxyfilename, baseurl)
        for item in compatibleVersions:
            if re.match(item, __version__):
                self.logger.debug("CRABClient version: %s" % (__version__))
                break 
        else:
            msg  = "%sWarning%s:" % (colors.RED, colors.NORMAL)
            msg += " Incompatible CRABClient version %s" % (__version__ )
            msg += "\nServer is saying that compatible versions are: %s" % [v.replace("\\", "") for v in compatibleVersions]
            self.logger.info(msg)


    def handleProxy(self, proxyOptsSetPlace):
        """ 
        Init the user proxy, and delegate it if necessary.
        """
        if not self.options.proxy:
            if self.cmdconf['initializeProxy']:
                self.proxy.setVOGroupVORole(self.voGroup, self.voRole)
                self.proxy.setMyProxyAccount(self.serverurl)
                self.proxyfilename = self.proxy.createNewVomsProxy(timeLeftThreshold = 720, \
                                                                   doProxyGroupRoleCheck = self.cmdconf['doProxyGroupRoleCheck'], \
                                                                   proxyCreatedByCRAB = self.proxyCreated, \
                                                                   proxyOptsSetPlace = proxyOptsSetPlace)
                if self.cmdconf['requiresREST']: ## If the command doesn't contact the REST, we can't delegate the proxy.
                    self.proxy.myproxyAccount = self.serverurl
                    baseurl = self.getUrl(self.instance, resource = 'info')
                    ## Get the DN of the task workers from the server.
                    all_task_workers_dns = server_info('delegatedn', self.serverurl, self.proxyfilename, baseurl)
                    for serverdn in all_task_workers_dns['services']:
                        self.proxy.setServerDN(serverdn)
                        self.proxy.setMyProxyServer('myproxy.cern.ch')
                        self.logger.debug("Registering user credentials for server %s" % serverdn)
                        self.proxy.createNewMyProxy(timeleftthreshold = 60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD, nokey = True)
        else:
            self.proxyfilename = self.options.proxy
            os.environ['X509_USER_PROXY'] = self.options.proxy
            self.logger.debug('Skipping proxy creation')


    def loadLocalCache(self, proxyOptsSetPlace, serverurl = None):
        """ 
        Loads the client cache and set up the server url
        """
        self.requestarea, self.requestname = getWorkArea(self.options.projdir)
        self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
        port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
        self.instance = self.cachedinfo['instance']
        self.serverurl = self.cachedinfo['Server'] + port
        msgadd = []
        if self.cmdconf['requiresProxyVOOptions'] and self.options.voGroup is None:
            self.voGroup = self.cachedinfo['voGroup']
            proxyOptsSetPlace['set_in']['group'] = "cache"
            msgadd.append("VO group '%s'" % (self.voGroup))
        if self.cmdconf['requiresProxyVOOptions'] and self.options.voRole is None:
            self.voRole = self.cachedinfo['voRole']
            proxyOptsSetPlace['set_in']['role'] = "cache"
            msgadd.append("VO role '%s'" % (self.voRole))
        if msgadd:
            msg = "Using %s as written in the request cache file for this task." % (" and ".join(msgadd))
            self.logger.debug(msg)


    def getConfiDict(self):
        """
        Load the CRAB cache file (~/.crab3). If it doesn't exist, create one.
        """
        crabCacheFileName = self.crabcachepath()
        if not os.path.isfile(crabCacheFileName):
            msg = "Could not find CRAB cache file %s; creating a new one." % (crabCacheFileName)
            self.logger.debug(msg)
            configdict = {'crab_project_directory': ''}
            crabCacheFileName_tmp = "%s.%s" % (crabCacheFileName, os.getpid())
            with open(crabCacheFileName_tmp, 'w') as fd:
                json.dump(configdict, fd)
            os.rename(crabCacheFileName_tmp, crabCacheFileName)
            return configdict
        try:
            msg = "Found CRAB cache file %s" % (crabCacheFileName)
            self.logger.debug(msg)
            with open(crabCacheFileName, 'r') as fd:
                configdict = json.load(fd)
        except ValueError:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Error loading CRAB cache file."
            msg += " Try to do 'rm -rf %s' and run the crab command again." % (crabCacheFileName)
            raise ConfigurationException(msg)
        if 'crab_project_directory' not in configdict:
            configdict['crab_project_directory'] = configdict.get('taskname', '')
        if 'taskname' in configdict:
            del configdict['taskname']
        return configdict


    def crabcachepath(self):
        if 'CRAB3_CACHE_FILE' in os.environ:
            if os.path.isabs(os.environ['CRAB3_CACHE_FILE']):
                return os.environ['CRAB3_CACHE_FILE']
            else:
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Invalid path in environment variable CRAB3_CACHE_FILE: %s" % (os.environ['CRAB3_CACHE_FILE'])
                msg += " Please export a valid full path."
                raise EnvironmentException(msg)
        else:
            return str(os.path.expanduser('~')) + '/.crab3'


    def updateCRABCacheFile(self):
        """
        Update the CRAB cache file.
        So far this file contains only the path of the last used CRAB project directory.
        """
        if self.cmdconf['requiresDirOption'] or getattr(self, 'requestarea', None):
            self.crab3dic['crab_project_directory'] = self.requestarea
            crabCacheFileName = self.crabcachepath()
            crabCacheFileName_tmp = "%s.%s" % (crabCacheFileName, os.getpid())
            with open(crabCacheFileName_tmp, 'w') as fd:
                json.dump(self.crab3dic, fd)
            os.rename(crabCacheFileName_tmp, crabCacheFileName)


    def __call__(self):
        self.logger.info("This is a 'nothing to do' command")
        raise NotImplementedError


    def terminate(self, exitcode):
        #We do not want to print logfile for each command...
        if exitcode < 2000:
            if getattr(self.options, 'dump', False) or getattr(self.options, 'xroot', False):
                self.logger.debug("Log file is %s" % os.path.abspath(self.logfile))
            else:
                self.logger.info("Log file is %s" % os.path.abspath(self.logfile))


    def setOptions(self):
        raise NotImplementedError


    def setSuperOptions(self):
        try:
            #add command related options
            self.setOptions()
        except NotImplementedError:
            pass

        self.parser.addCommonOptions(self.cmdconf)


    def validateOptions(self):
        """
        __validateOptions__

        Validate the command line options of the command.
        Raise a ConfigurationException in case of error; don't do anything if ok.
        """

        if self.cmdconf['requiresDirOption']:
            if self.options.projdir is None:
                if len(self.args) > 1:
                    msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                    msg += " 'crab %s' command accepts at most 1 argument (a path to a CRAB project directory), %d given." % (self.name, len(self.args))
                    raise ConfigurationException(msg)
                elif len(self.args) == 1 and self.args[0]:
                    self.options.projdir = self.args.pop(0)
                elif self.cmdconf['useCache'] and self.crab3dic.get('crab_project_directory'):
                    self.options.projdir = str(self.crab3dic['crab_project_directory'])
            if self.options.projdir is None:
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Please indicate the CRAB project directory with --dir=<project-directory>."
                ex = MissingOptionException(msg)
                ex.missingOption = "task"
                raise ex
            if not os.path.isdir(self.options.projdir):
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " %s is not a valid CRAB project directory." % (self.options.projdir)
                raise ConfigurationException(msg)

        ## If the command does not take any arguments, but some arguments were passed,
        ## clear the arguments list and give a warning message saying that the given
        ## arguments will be ignored.
        if not self.cmdconf['acceptsArguments'] and len(self.args):
            msg  = "%sWarning%s:" % (colors.RED, colors.NORMAL)
            msg += " 'crab %s' command takes no arguments, %d given." % (self.name, len(self.args))
            msg += " Ignoring arguments %s." % (self.args)
            self.logger.warning(msg)
            self.args = []
