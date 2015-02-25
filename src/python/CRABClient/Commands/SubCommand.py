import os
import re
import imp 
import json
import types
from optparse import OptionParser
from ast import literal_eval

from CRABClient.ClientUtilities import loadCache, getWorkArea, server_info, createWorkArea
from CRABClient.ClientUtilities import BASEURL, SERVICE_INSTANCES
from CRABClient import SpellChecker
import CRABClient.Emulator
from CRABClient.ClientExceptions import ConfigurationException, MissingOptionException, EnvironmentException, UnknownOptionException
from CRABClient.ClientMapping import renamedParams, commandsConfiguration, configParametersInfo, getParamDefaultValue
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.__init__ import __version__
from CRABClient.ClientUtilities import colors
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
            raise ConfigurationException("Configuration file '%s' not found." % configname)
        self.logger.info('Will use configuration file %s.' % configname)
        try:
            self.logger.debug('Loading configuration')
            self.configuration = loadConfigurationFile(os.path.abspath(configname))
            ## Overwrite configuration parameters passed as arguments in the command line. 
            if overrideargs:
                for singlearg in overrideargs:
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
        except RuntimeError, re:
            configmsg  = "Configuration syntax error:\n%s" % (self._extractReason(configname, re))
            configmsg += "\nPlease refer to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3CommonErrors#Configuration_syntax_error."
            configmsg += "\nSee the ./crab.log file for more details."
            configmsg += "\nThe documentation about the CRAB configuration file can be found in"
            configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile."
            raise ConfigurationException(configmsg)
        else:
            if not valid:
                configmsg += "\nThe documentation about the CRAB configuration file can be found in"
                configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile."
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
        except Exception, ex:
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
            if len(old_param.split('.')) != 2 or len(new_param.split('.')) != 2:
                continue
            old_param_section, old_param_name = old_param.split('.')
            if hasattr(self.configuration, old_param_section) and hasattr(getattr(self.configuration, old_param_section), old_param_name):
                msg = "Invalid CRAB configuration: Parameter %s has been renamed to %s; please change your configuration file accordingly." % (old_param, new_param)
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
            except AttributeError, ex:
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
        raise ConfigurationException('Error: only %s instances can be used.' %str(SERVICE_INSTANCES.keys()))


    def __init__(self, logger, cmdargs = None, disable_interspersed_args = False):
        """
        Initialize common client parameters
        """
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__
        self.usage = "usage: %prog " + self.name + " [options] [args]"
        self.logger = logger
        self.logfile = self.logger.logfile
        self.logger.debug("Executing command: '%s'" % str(self.name))

        self.proxy = None
        self.restClass = CRABClient.Emulator.getEmulator('rest')
        self.cmdconf = commandsConfiguration.get(self.name)
        self.crab3dic = self.getConfiDict()

        self.parser = OptionParser(description = self.__doc__, usage = self.usage, add_help_option = True)
        ## TODO: check on self.name should be removed (creating another abstraction in between or refactoring this)
        if disable_interspersed_args:
            self.parser.disable_interspersed_args()
        self.setSuperOptions()
        ## Parse the command line parameters.
        cmdargs = cmdargs or []
        (self.options, self.args) = self.parser.parse_args(cmdargs)

        ## Validate the command line parameters before initializing the proxy.
        self.validateOptions()

        ## Retrieve VO role/group from the command line parameters.
        self.voRole  = self.options.voRole  if self.options.voRole  is not None else ''
        self.voGroup = self.options.voGroup if self.options.voGroup is not None else ''

        ## Create the object that will do the proxy operations. We ignore the VO role/group and
        ## server URL in the initialization, because they are not used until we do the proxy
        ## delegation to myproxy server. And the delegation happends in handleProxy(), which is
        ## called after we load the configuration file and retrieve the final values for those
        ## parameters. handleProxy() takes care of passing those parameters to self.proxy.
        self.proxy = CredentialInteractions('', '', self.voRole, self.voGroup, self.logger, '')

        ## Create a new proxy (if there isn't a valid one already) with current VO role/group.
        ## The VO role/group are not relevant for the proxy. We only want to check that the
        ## user doesn't expect the proxy to have different VO role/group than those specified
        ## in the configuration file, but this check is done later in handleProxy() after we
        ## load the configuration file.
        self.proxy_created = False
        if not self.options.proxy and self.cmdconf['initializeProxy']:
            self.proxy_created = self.proxy.createNewVomsProxySimple(time_left_threshold = 720)

        ## If we get an input configuration file:
        if hasattr(self.options, 'config') and self.options.config is not None:
            ## Load the configuration file and validate it.
            self.loadConfig(self.options.config, self.args)
            ## Create the CRAB project directory.
            self.requestarea, self.requestname, self.logfile = createWorkArea(self.logger,
                                                                              getattr(self.configuration.General, 'workArea', None),
                                                                              getattr(self.configuration.General, 'requestName', None))
            ## If VO role/group were not given in the command options, get them from the configuration file.
            ## If they are specified in both places, print a message saying that the command options will be used.
            if self.options.voRole  is None and hasattr(self.configuration, 'User'):
                self.voRole  = getattr(self.configuration.User, 'voRole',  '')
            if self.options.voGroup is None and hasattr(self.configuration, 'User'):
                self.voGroup = getattr(self.configuration.User, 'voGroup', '')
            if (self.options.voRole  is not None) and (hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voRole' )):
                msg = "Ignoring the VO role specified in the configuration file. Using VO role \"%s\" "
                if self.voRole == '': msg += "(i.e. no VO role) "
                msg += "as specified in the command line."
                self.logger.info(msg % self.voRole)
            if (self.options.voGroup is not None) and (hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voGroup')):
                msg = "Ignoring the VO group specified in the configuration file. Using VO group \"%s\" "
                if self.voGroup == '': msg += "(i.e. no VO group) "
                msg += "as specified in the command line."
                self.logger.info(msg % self.voGroup)

        ## If we get an input task, we load the cache and set the server URL and VO role/group from it.
        if hasattr(self.options, 'task') and self.options.task:
            self.loadLocalCache()

        ## If the server url isn't already set, we check the args and then the config.
        if not hasattr(self, 'serverurl') and self.cmdconf['requiresREST']:
            self.instance, self.serverurl = self.serverInstance()
        elif not self.cmdconf['requiresREST']:
            self.instance, self.serverurl = None, None

        ## Update (or create) the .crab3 cache file.
        self.updateCrab3()

        ## At this point there should be a valid proxy, because we have already checked that and
        ## eventually created a new one. If the proxy was not created by CRAB, we check that the
        ## VO role/group in the proxy are the same as specified by the user in the configuration
        ## file (or in the command line options). If it is not, we ask the user if he wants to 
        ## overwrite the current proxy. If he doesn't want to overwrite it, we don't continue 
        ## and ask him to provide the VO role/group as in the existing proxy. 
        ## Finally, delegate the proxy to myproxy server.
        self.handleProxy()

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
        compatibleversion = server_info('version', self.serverurl, self.proxyfilename, baseurl)
        compatible = False
        for item in compatibleversion:
            if re.match(item, __version__):
                self.logger.debug("CRABClient version: %s Compatible"  % __version__)
                compatible = True
                break 
        if not compatible:
            self.logger.info("%sWarning%s: Incompatible CRABClient version \"%s\" " % (colors.RED, colors.NORMAL , __version__ ))
            self.logger.info("Server is saying that compatible versions are: %s"  % [v.replace("\\", "") for v in compatibleversion])


    def handleProxy(self):
        """ 
        Init the user proxy, and delegate it if necessary.
        """
        if not self.options.proxy:
            if self.cmdconf['initializeProxy']:
                self.proxy.setVOGroupVORole(self.voGroup, self.voRole)
                self.proxy.setMyProxyAccount(self.serverurl)
                _, self.proxyfilename = self.proxy.createNewVomsProxy(time_left_threshold = 720, proxy_created_by_crab = self.proxy_created)
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

    def loadLocalCache(self, serverurl = None):
        """ 
        Loads the client cache and set up the server url
        """
        self.requestarea, self.requestname = getWorkArea( self.options.task )
        self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
        port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
        self.instance = self.cachedinfo['instance']
        self.serverurl = self.cachedinfo['Server'] + port
        self.voRole = self.cachedinfo['voRole'] #if not self.options.voRole else self.options.voRole
        self.voGroup = self.cachedinfo['voGroup'] #if not self.options.voGroup else self.options.voGroup


    def getConfiDict(self):

        crab3fdir = self.crabcachepath()
        if not os.path.isfile(crab3fdir):
            self.logger.debug("Could not find %s file; creating a new one" % crab3fdir)
            crab3f = open(crab3fdir,'w')
            #creating a user dict, do add for future use
            configdict = { "taskname" : None }
            json.dump(configdict,crab3f)
            crab3f.close()
            return configdict
        else:
            try :
                self.logger.debug("Found %s file" % crab3fdir)
                crab3f = open(crab3fdir,'r')
                configdict = json.load(crab3f)
                crab3f.close()
            except ValueError:
                self.logger.info('%sError%s: Error in reading json file\nTry to do "rm -rf ~/.crab3", and run the crab command again'% (colors.RED, colors.NORMAL))
                raise ConfigurationException
            return configdict


    def crabcachepath(self):

         if 'CRAB3_CACHE_FILE' in os.environ and os.path.isabs(os.environ['CRAB3_CACHE_FILE']):
             return os.environ['CRAB3_CACHE_FILE']
         elif 'CRAB3_CACHE_FILE' in os.environ and not os.path.isabs(os.environ['CRAB3_CACHE_FILE']):
             msg = '%sError%s: An invalid path is use for CRAB3_CACHE_FILE, please export a valid full path' % (colors.RED, colors.NORMAL)
             raise EnvironmentException(msg)
         else:
             return str(os.path.expanduser('~')) + '/.crab3'


    def updateCrab3(self):
        if self.cmdconf['requiresTaskOption'] or hasattr(self,'requestname') and self.requestname != None:
            crab3fdir=self.crabcachepath()
            crab3f = open(crab3fdir, 'w')
            self.crab3dic['taskname'] = self.requestarea
            json.dump(self.crab3dic, crab3f)
            crab3f.close()


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
            self.setOptions()
        except NotImplementedError:
            pass

        self.parser.add_option("--proxy",
                               dest = "proxy",
                               default = False,
                               help = "Use the given proxy. Skip Grid proxy creation and myproxy delegation.")

        if self.cmdconf['requiresTaskOption']:
            self.parser.add_option("-d", "--dir",
                                   dest = "task",
                                   default = None,
                                   help = "Path to the crab project directory for which the crab command should be executed.")
            self.parser.add_option("-t", "--task",
                                   dest = "oldtask",
                                   default = None,
                                   help = "Deprecated option renamed to -d/--dir in CRAB v3.3.12.")

        self.parser.add_option("--voRole",
                               dest = "voRole",
                               default = None)

        self.parser.add_option("--voGroup",
                               dest = "voGroup",
                               default = None)
        if self.cmdconf['requiresREST']:

            self.parser.add_option("--instance",
                                   dest = "instance",
                                   type = "string",
                                   help = "Running instance of CRAB service. Valid values are %s." %str(SERVICE_INSTANCES.keys()))


    def validateOptions(self):
        """
        __validateOptions__

        Validate the command line options of the command.
        Raise a ConfigurationException in case of error; don't do anything if ok.
        """

        if self.cmdconf['requiresTaskOption'] and self.options.oldtask is not None:
            msg = "CRAB command line option error: the option -t/--task has been renamed to -d/--dir."
            raise UnknownOptionException(msg)

        if self.cmdconf['requiresTaskOption'] and self.options.task is None:
            if len(self.args) == 1 and self.args[0]:
                self.options.task = self.args[0]
            elif self.cmdconf['useCache'] and self.crab3dic['taskname'] != None:
                self.options.task = self.crab3dic['taskname']
            else:
                raise MissingOptionException('ERROR: Task option is required.')
