import os
import re
import imp
import json
import types
from ast import literal_eval
from datetime import timedelta

from WMCore.Configuration import loadConfigurationFile, Configuration

from ServerUtilities import SERVICE_INSTANCES

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.CRABOptParser import CRABCmdOptParser
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.ClientUtilities import loadCache, getWorkArea, server_info, createWorkArea, execute_command
from CRABClient.ClientExceptions import (ConfigurationException, MissingOptionException,
                                         EnvironmentException, CachefileNotFoundException,
                                         RucioClientException)
from CRABClient.ClientMapping import (renamedParams, commandsConfiguration, configParametersInfo,
                                      getParamDefaultValue, deprecatedParams)
from CRABClient.UserUtilities import getUsername

#if certificates in myproxy expires in less than RENEW_MYPROXY_THRESHOLD days renew them
RENEW_MYPROXY_THRESHOLD = 15


class ConfigCommand:
    """
    Commands which needs to load the configuration file (e.g.: submit, publish) must subclass ConfigCommand
    Provides methods for loading the configuration file handling the errors
    """
    def __init__(self):
        self.configuration = None
        self.logger = None

    def loadConfig(self, configname, overrideargs=None):
        """
        Load the configuration file
        """
        # If the configuration is alredy an object it doesn't need to be loaded from the file.
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
            # Overwrite configuration parameters passed as arguments in the command line.
            if overrideargs:
                for singlearg in overrideargs:
                    # The next line is needed, because we allow the config to be passed as argument
                    # instead via the --config option.
                    if singlearg == configname: continue
                    if len(singlearg.split('=', 1)) == 1:
                        self.logger.info("Wrong format in command-line argument '%s'. Expected format is <section-name>.<parameter-name>=<parameter-value>." % (singlearg))
                        if len(singlearg) > 1 and singlearg[0] == '-':
                            self.logger.info("If the argument '%s' is an option to the %s command, try 'crab %s %s [value for %s option (if required)] [arguments]'." \
                                             % (singlearg, self.__class__.__name__, self.__class__.__name__, singlearg, singlearg))
                        raise ConfigurationException("ERROR: Wrong command-line format.")
                    fullparname, parval = singlearg.split('=', 1)
                    # now supporting just one sub params, eg: Data.inputFiles, User.email, ...
                    parnames = fullparname.split('.', 1)
                    if len(parnames) == 1:
                        self.logger.info("Wrong format in command-line argument '%s'. Expected format is <section-name>.<parameter-name>=<parameter-value>." % (singlearg))
                        raise ConfigurationException("ERROR: Wrong command-line format.")
                    self.configuration.section_(parnames[0])
                    parType = configParametersInfo.get(fullparname, {}).get('type', 'undefined')
                    if parType in ['undefined', 'StringType']:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("\'%s\'" % parval))
                        self.logger.debug("Overriden parameter %s with '%s'" % (fullparname, parval))
                    else:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("%s" % parval))
                        self.logger.debug("Overriden parameter %s with %s" % (fullparname, parval))
            valid, configmsg = self.validateConfig() # Subclasses of SubCommand overwrite this method if needed.
        except RuntimeError as runErr:
            configmsg = "Error while loading CRAB configuration:\n%s" % (self._extractReason(configname, runErr))
            configmsg += "\nPlease refer to https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ"
            configmsg += "\nSee the ./crab.log file for more details."
            configmsg += "\nThe documentation about the CRAB configuration file can be found in"
            configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
            raise ConfigurationException(configmsg)
        else:
            if not valid:
                configmsg += "\nThe documentation about the CRAB configuration file can be found in"
                configmsg += " https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile"
                raise ConfigurationException(configmsg)


    def _extractReason(self, configname, runErr):
        """
        To call in case of error loading the configuration file
        Get the reason of the failure without the stacktrace. Put the stacktrace in the crab.log file
        """
        #get only the error wihtout the stacktrace
        msg = str(runErr)
        filename = os.path.abspath(configname)
        cfgBaseName = os.path.basename(filename).replace(".py", "")
        cfgDirName = os.path.dirname(filename)
        if  not cfgDirName:
            modPath = imp.find_module(cfgBaseName)
        else:
            modPath = imp.find_module(cfgBaseName, [cfgDirName])
        try:
            imp.load_module(cfgBaseName, modPath[0], modPath[1], modPath[2])
        except Exception as ex:
            msg = str(ex)

        return msg


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there.
        Not all the commands require a configuration.
        """
        # Check that the configuration object has the sections we expect it to have.
        # (WMCore already checks that attributes added to the configuration object are of type ConfigSection.)
        # Even if not all configuration sections need to be there, we anyway request
        # the user to add all the sections in the configuration file.
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
        for old_param, new_param in renamedParams.items():
            if len(old_param.split('.')) != 2 or len(new_param['newParam'].split('.')) != 2:
                continue
            old_param_section, old_param_name = old_param.split('.')
            if hasattr(self.configuration, old_param_section) and hasattr(getattr(self.configuration, old_param_section), old_param_name):
                msg = "Invalid CRAB configuration: Parameter %s has been renamed to %s" % (old_param, new_param['newParam'])
                if new_param['version'] != None:
                    msg += " starting from CRAB %s" % (new_param['version'])
                msg += "; please change your configuration file accordingly."
                return False, msg

        # Check if there are unknown or deprecated parameters
        # the old "try to suggest" is broken in python3 and we can surely live without it
        all_config_params = configParametersInfo.keys()
        for section in self.configuration.listSections_():
            for attr in getattr(self.configuration, section).listSections_():
                param = (section + '.' + attr)
                if param in deprecatedParams:
                    msg = "Invalid CRAB confgituation: Parameter %s has been deprecated. Please remove it" % param
                    return False, msg
                if not param in all_config_params:
                    msg = "Invalid CRAB configuration: Parameter %s is not known." % param
                    return False, msg

        # the validation code below does not run in python3 and porting proved not
        # straightforward (see https://github.com/dmwm/CRABClient/issues/5004  and
        # in particulr https://github.com/dmwm/CRABClient/issues/5004#issuecomment-990073439 ).
        # Moreover, experience indicates that syntax errors in crabConfig.py are easily
        # spotted anyhow. Therefore current approach is to skip validation and
        # let it to python to report erros as exceptions.
        return True, "Valid configuration"

        ## Check that each parameter specified in the configuration file is of the
        ## type specified in the configuration map.
        ## Check that, if a parameter is a required one and it has no default value,
        ## then it must be specified in the configuration file.
        for paramName, paramInfo in configParametersInfo.items():
            requiredTypeName = paramInfo['type']
            try:
                requiredType = getattr(types, requiredTypeName)
            except AttributeError:
                msg = "Invalid type %s specified in CRABClient configuration mapping for parameter %s." % (requiredTypeName, paramName)
                return False, msg
            attrs = paramName.split('.')
            obj = self.configuration
            while attrs and obj is not None:
                obj = getattr(obj, attrs.pop(0), None)
            if obj is not None:
                if not isinstance(obj, requiredType):
                    msg = "Invalid CRAB configuration: Parameter %s requires a value of type %s (while a value of type %s was given)." \
                          % (paramName, str(requiredType), str(type(obj)))
                    if paramName == "Data.totalUnits" and isinstance(obj, float):
                        continue
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

    #### These options can be overrhidden if needed ####
    # setting visible = False doesn't allow the sub-command to be called from CLI
    visible = True
    proxyfilename = None
    shortnames = []
    usage = "usage: %prog [command-options] [args]"
    #Default command name is the name of the command class, but it is also possible to set the name attribute in the subclass
    #if the command name does not correspond to the class name (getlog => get-log)

    def __init__(self, logger, cmdargs=None, disable_interspersed_args=False):
        """
        Initialize common client parameters
        """
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__

        ConfigCommand.__init__(self)
        # The command logger.
        self.logger = logger
        self.logfile = self.logger.logfile

        stdout, _, _ = execute_command(command='uname -a')
        localSystem = stdout.strip()
        try:
            localOS, _, _ = execute_command('grep PRETTY_NAME /etc/os-release')
            localOS = localOS.strip().split('=')[1].strip('"')
        except Exception as ex:  # pylint: disable=unused-variable
            try:
                localOS, _, _ = execute_command(command='lsb_release -d')
                localOS = localOS.strip().split(':')[1].strip()
            except Exception as ex:  # pylint: disable=unused-variable
                localOS = "Unknown Operating System"
        self.logger.debug("CRAB Client version: %s", __version__)
        self.logger.debug("Running on: " + localSystem + " - " + localOS)
        self.logger.debug("Executing command: '%s'" % str(self.name))

        # Get the command configuration.
        self.cmdconf = commandsConfiguration.get(self.name)
        if not self.cmdconf:
            raise RuntimeError("Canot find command %s in commandsConfiguration inside ClientMapping. Are you a developer"
                               "trying to add a command without it's correspondant configuration?" % self.name)

        # Get the CRAB cache file.
        self.cachedinfo = None
        self.crab3dic = self.getConfiDict()

        # The options parser.
        self.parser = CRABCmdOptParser(self.name, self.__doc__, disable_interspersed_args)

        # Define the command options.
        self.setSuperOptions()

        # Parse the command options/arguments.
        cmdargs = cmdargs or []
        (self.options, self.args) = self.parser.parse_args(cmdargs)

        self.transferringIds = None
        self.dest = None

        # Validate first the SubCommand options
        SubCommand.validateOptions(self)
        # then the config option for the submit command
        self.validateConfigOption()

        # Get the VO group/role from the command options (if the command requires these
        # options).
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

        # Create the object that will do the proxy operations. We don't really care
        # what VO role and group and server URL we pass to the constructor, because
        # these are not used until we do the proxy delegation to the myproxy server.
        # And this happens in handleProxy(), which is called after we load the
        # configuration file and retrieve the final values for those parameters.
        # handleProxy() takes care of passing those parameters to self.credentialHandler
        self.credentialHandler = CredentialInteractions(self.logger)

        # If the user didn't use the --proxy command line option, and if there isn't a
        # valid proxy already, we will create a new one with the current VO role and group
        # (as commented above, we don't really care what are the VO role and group so
        # far).
        self.proxyCreated = False

        # If there is an input configuration file:
        if hasattr(self.options, 'config') and self.options.config is not None:
            proxyOptsSetPlace['for_set_use'] = "config"
            # Load the configuration file and validate it.
            self.loadConfig(self.options.config, self.args)
            # Create the CRAB project directory.
            self.requestarea, self.requestname, self.logfile = createWorkArea(
                self.logger, getattr(self.configuration.General, 'workArea', None),
                getattr(self.configuration.General, 'requestName', None))
            # Get the VO group/role from the configuration file.
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

        # If the VO group/role was not given in the command options, take it from the request cache.
        if self.cmdconf['requiresDirOption']:
            self.setCachedProxy(proxyOptsSetPlace)

        # If the server URL isn't already set, we check the args and then the config.
        if not hasattr(self, 'serverurl') and self.cmdconf['requiresREST']:
            self.instance, self.serverurl = self.serverInstance()
        elif not self.cmdconf['requiresREST']:
            self.instance, self.serverurl = None, None

        # Update (or create) the CRAB cache file.
        self.updateCRABCacheFile()

        # At this point we check if there is a valid proxy, and
        # eventually create a new one. If the proxy was not created by CRAB, we check that the
        # VO role/group in the proxy are the same as specified by the user in the configuration
        # file (or in the command line options). If it is not, we ask the user if he wants to
        # overwrite the current proxy. If he doesn't want to overwrite it, we don't continue
        # and ask him to provide the VO role/group as in the existing proxy.
        # Finally, delegate the proxy to myproxy server.
        self.handleVomsProxy(proxyOptsSetPlace)

        # only if this command talks to the REST we create a CRABRest object to communicate with CRABServer
        # and check/upate credentials on myproxy
        # this is usually the first time that a call to the server is made, so where Emulator('rest') is initialized
        # arguments to Emulator('rest') call must match those for HTTPRequest.__init__ in RESTInteractions.py
        #server = CRABClient.Emulator.getEmulator('rest')(url=serverurl, localcert=proxyfilename, localkey=proxyfilename,
        #          retry=2, logger=logger)
        if self.cmdconf['requiresREST']:
            crabRest = CRABClient.Emulator.getEmulator('rest')
            self.crabserver = crabRest(hostname=self.serverurl, localcert=self.proxyfilename, localkey=self.proxyfilename,
                                        retry=2, logger=self.logger, verbose=False)
            self.crabserver.setDbInstance(self.instance)
            # prepare also a test crabserver instance which will send tarballs to S3
            self.s3tester = crabRest(hostname='cmsweb-testbed.cern.ch',
                                      localcert=self.proxyfilename, localkey=self.proxyfilename,
                                      retry=0, logger=self.logger, verbose=False)
            self.s3tester.setDbInstance('preprod')
            self.handleMyProxy()

        # Validate the command options
        self.validateOptions()
        self.validateOptions()

        # Log user command and options used for debuging purpose.
        self.logger.debug('Command use: %s' % self.name)
        self.logger.debug('Options use: %s' % cmdargs)
        if self.cmdconf['requiresREST']:
            self.checkversion()
            self.defaultApi = 'workflow'
        self.logger.debug("Instance is %s" %(self.instance))
        self.logger.debug("Server base url is %s" %(self.serverurl))
        if self.cmdconf['requiresREST']:
            self.logger.debug("Command api %s" %(self.defaultApi))

    def serverInstance(self):
        """
        Deriving the correct instance to use and the server url. Client is allowed to propagate the instance name and corresponding url
        via crabconfig.py or crab option --instance. The variable passed via crab option will always be used over the variable
        in crabconfig.py.
        """
        if hasattr(self.options, 'instance') and self.options.instance is not None:
            if hasattr(self, 'configuration') and hasattr(self.configuration, 'General') and hasattr(self.configuration.General, 'instance') and self.configuration.General.instance is not None:
                msg = "%sWarning%s: CRAB configuration parameter General.instance is overwritten by the command option --instance;" % (colors.RED, colors.NORMAL)
                msg += " %s intance will be used." % (self.options.instance)
                self.logger.info(msg)
            instance = self.options.instance
        elif hasattr(self, 'configuration') and hasattr(self.configuration, 'General') and hasattr(self.configuration.General, 'instance') and self.configuration.General.instance is not None:
            instance = self.configuration.General.instance
        else:
            instance = getParamDefaultValue('General.instance')

        # validate instance
        if not instance in SERVICE_INSTANCES:
            msg = 'Invalid value "%s" for configuration.General.instance\n' % instance
            msg += 'valid values are %s ' % SERVICE_INSTANCES.keys()
            raise ConfigurationException(msg)

        if instance != 'other':
            self.restHost = SERVICE_INSTANCES[instance]['restHost']
            self.dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
        else:
            self.restHost = self.configuration.General.restHost
            self.dbInstance = self.configuration.General.dbInstance

        # attempt at backward cmpatibility
        #self.serverurl = self.serverHost
        #self.instance = self.dbInstance

        return self.dbInstance, self.restHost


    def checkversion(self):
        compatibleVersions = server_info(crabserver=self.crabserver, subresource='version')
        for item in compatibleVersions:
            if re.match(item, __version__):
                self.logger.debug("CRABClient version: %s" % (__version__))
                break
        else:
            msg = "%sWarning%s:" % (colors.RED, colors.NORMAL)
            msg += " Incompatible CRABClient version %s" % (__version__)
            msg += "\nServer is saying that compatible versions are: %s" % [v.replace("\\", "") for v in compatibleVersions]
            self.logger.info(msg)


    def handleVomsProxy(self, proxyOptsSetPlace):
        """
        Make sure that there is a valid VOMS proxy
        All CRABClient commands require a proxy as of Oct 2023.
        :param proxyOptsSetPlace: a complicated dictionary to keep track of VOMS group/roles in options/proxyfile/requestcache
                                  used by  CredentialInteractions/createNewVomsProxy()
        :return: nothing
                    a dictionary with proxy info is added to self as self.proxyInfo and
                    the file name of a valid proxy is addded as self.proxyfilename
        """
        if self.options.proxy:  # user passed a proxy as option
            self.proxyfilename = self.options.proxy
            os.environ['X509_USER_PROXY'] = self.options.proxy
            self.logger.debug('Skipping proxy creation')
            return
        self.credentialHandler.setVOGroupVORole(self.voGroup, self.voRole)
        proxyInfo = self.credentialHandler.createNewVomsProxy(timeLeftThreshold=720, \
                                                           proxyCreatedByCRAB=self.proxyCreated, \
                                                           proxyOptsSetPlace=proxyOptsSetPlace)
        self.proxyfilename = proxyInfo['filename']
        return

    def handleMyProxy(self):
        """
        check myproxy credential and delegate again it if necessary.
        takes no input and returns no output, bur raises exception if delegation failed
        """
        if not self.cmdconf['requiresREST']:  # If the command doesn't contact the REST, we can't delegate the proxy.
            return
        if self.options.proxy:  # if user passed a proxy as option we don't contact myproxy
            return

        if not self.options.proxy:
            # Get the DN of the task workers from the server.
            all_task_workers_dns = server_info(self.crabserver, subresource='delegatedn')
            for authorizedDNs in all_task_workers_dns['services']:
                self.credentialHandler.setRetrievers(authorizedDNs)
                self.logger.debug("Registering user credentials on myproxy for %s" % authorizedDNs)
                try:
                    (credentialName, myproxyTimeleft) = \
                        self.credentialHandler.createNewMyProxy(timeleftthreshold=60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD)
                    p1 = True
                    msg1 = "Credential exists on myproxy: username: %s  - validity: %s" %\
                           (credentialName, str(timedelta(seconds=myproxyTimeleft)))
                except Exception as ex:
                    p1 = False
                    msg1 = "Error trying to create credential:\n %s" % str(ex)
                if (not p1):
                    from CRABClient.ClientExceptions import ProxyCreationException
                    raise ProxyCreationException("Problems delegating My-proxy.\n%s" % msg1)
                self.logger.debug("Result of myproxy credential check:\n  %s", msg1)


    def loadLocalCache(self):
        """
        Loads the client cache and set up the server url
        """
        self.requestarea, self.requestname = getWorkArea(self.options.projdir)
        try:
            self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
            port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
            self.instance = self.cachedinfo['instance']
            self.serverurl = self.cachedinfo['Server'] + port
        except CachefileNotFoundException as ex:
            if self.cmdconf['requiresLocalCache']:
                raise ex


    def setCachedProxy(self, proxyOptsSetPlace):
        """
        Set the proxy parameters from the cache if not specified otherwise
        """
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
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
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
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
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

    def initRucioClient(self, lfn):
        if self.cmdconf['requiresRucio']:
            if os.environ.get('RUCIO_HOME', None):
                from ServerUtilities import getRucioAccountFromLFN
                from rucio.client import Client
                from rucio.common.exception import RucioException
                if hasattr(self.options, 'userlfn') and self.options.userlfn is not None\
                    and (self.options.userlfn.startswith('/store/user/rucio/')
                         or self.options.userlfn.startswith('/store/group/rucio/')):
                    account = getRucioAccountFromLFN(self.options.userlfn)
                else:
                    account = getUsername(self.proxyfilename, logger=self.logger)
                os.environ['RUCIO_ACCOUNT'] = account
                try:
                    self.rucio = Client()
                    me = self.rucio.whoami()
                    self.logger.info('Rucio client intialized for account %s' % me['account'])
                except RucioException as e:
                    msg = "Cannot initialize Rucio Client. Error: %s" % str(e)
                    raise RucioClientException(msg)
            else:
                self.rucio = None


    def __call__(self):
        """
        this needs to be implemented by each command class which subclassed SubCommand
        call signature is always __call__(self)
        the command must either raise an exception for the caller to catch or
        return a dictionary of the format
        {'commandStatus': status, key: val, key: val ....}
        where status can have the values 'SUCCESS' or 'FAILED'
        and the other keys and values are command dependent !
        """
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
                    msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                    msg += " 'crab %s' command accepts at most 1 argument (a path to a CRAB project directory), %d given." % (self.name, len(self.args))
                    raise ConfigurationException(msg)
                elif len(self.args) == 1 and self.args[0]:
                    self.options.projdir = self.args.pop(0)
                elif self.cmdconf['useCache'] and self.crab3dic.get('crab_project_directory'):
                    self.options.projdir = str(self.crab3dic['crab_project_directory'])
            if self.options.projdir is None:
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Please indicate the CRAB project directory with --dir=<project-directory>."
                ex = MissingOptionException(msg)
                ex.missingOption = "task"
                raise ex
            if not os.path.isdir(self.options.projdir):
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " %s is not a valid CRAB project directory." % (self.options.projdir)
                raise ConfigurationException(msg)

            # If an input project directory was given, load the request cache and take the
            # server URL from it.
            self.loadLocalCache()

        # If the command does not take any arguments, but some arguments were passed,
        # clear the arguments list and give a warning message saying that the given
        # arguments will be ignored.
        if not self.cmdconf['acceptsArguments'] and len(self.args):
            msg = "%sWarning%s:" % (colors.RED, colors.NORMAL)
            msg += " 'crab %s' command takes no arguments, %d given." % (self.name, len(self.args))
            msg += " Ignoring arguments %s." % (self.args)
            self.logger.warning(msg)
            self.args = []

    def validateConfigOption(self):
        pass
