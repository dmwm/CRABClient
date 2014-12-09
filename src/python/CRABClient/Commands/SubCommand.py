import os
import imp
import json
import types
from optparse import OptionParser
from ast import literal_eval

from CRABClient.client_utilities import loadCache, getWorkArea, server_info, createWorkArea
from CRABClient.client_utilities import BASEURL, SERVICE_INSTANCES
import CRABClient.Emulator
from CRABClient.client_exceptions import ConfigurationException, MissingOptionException, EnvironmentException, UnknownOptionException
from CRABClient.ClientMapping import parameters_mapping, commands_configuration
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.__init__ import __version__
from CRABClient.client_utilities import colors
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
        #The configuration is alredy an object and don't need to be loaded from the file
        if isinstance(configname, Configuration):
            self.configuration = configname
            valid, configmsg = self.validateConfig()
            if not valid:
                raise ConfigurationException(configmsg)
            return

        if not os.path.isfile(configname):
            raise ConfigurationException("Configuration file '%s' not found" % configname)
        self.logger.info('Will use configuration file %s' % configname)
        try:
            self.logger.debug('Loading configuration')
            self.configuration = loadConfigurationFile(os.path.abspath(configname))
            ## Overwrite configuration parameters passed as arguments in the command line. 
            if overrideargs:
                for singlearg in overrideargs:
                    if singlearg == configname: continue
                    if len(singlearg.split('=',1)) == 1:
                        self.logger.info('Wrong format in command-line argument \'%s\'. Expected format is <section-name>.<parameter-name>=<parameter-value>.' % singlearg)
                        if len(singlearg) > 1 and singlearg[0] == '-':
                            self.logger.info('If the argument \'%s\' is an option to the %s command, try \'crab %s %s [value for %s option (if required)] [arguments]\'.' \
                                             % (singlearg, self.__class__.__name__, self.__class__.__name__, singlearg, singlearg))
                        raise ConfigurationException('ERROR: Wrong command-line format.')
                    fullparname, parval = singlearg.split('=',1)
                    # now supporting just one sub params, eg: Data.inputFiles, User.email, ...
                    parnames = fullparname.split('.', 1)
                    if len(parnames) == 1:
                        self.logger.info('Wrong format in command-line argument \'%s\'. Expected format is <section-name>.<parameter-name>=<parameter-value>' % singlearg)
                        raise ConfigurationException('ERROR: Wrong command-line format.')
                    self.configuration.section_(parnames[0])
                    type = 'undefined'
                    for k in parameters_mapping['on-server'].keys():
                        if fullparname in parameters_mapping['on-server'][k]['config']:
                            type = parameters_mapping['on-server'][k]['type']
                            break
                    if type in ['undefined','StringType']:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("\'%s\'" % parval))
                        self.logger.debug('Overriden parameter %s with \'%s\'' % (fullparname, parval))
                    else:
                        setattr(getattr(self.configuration, parnames[0]), parnames[1], literal_eval("%s" % parval))
                        self.logger.debug('Overriden parameter %s with %s' % (fullparname, parval))
            valid, configmsg = self.validateConfig() #subclasses of SubCommand overrhide this if needed
        except RuntimeError, re:
            msg = self._extractReason(configname, re)
            raise ConfigurationException("Configuration syntax error:\n%s\nPlease refer to <https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3CommonErrors#Configuration_syntax_error>\nSee the ./crab.log file for more details" % msg)
        else:
            ## file is there, check if it is ok
            if not valid:
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

        Checking if needed input parameters are there
        Not all the commands requires a configuration
        """
        ## Check that each parameter specified in the configuration file is of the 
        ## type specified in the configuration map.
        ## Check that, if a parameter is a required one and it has no default value,
        ## then it must be specified in the configuration file.
        for param in parameters_mapping['on-server']:
            for param_full_config_name in parameters_mapping['on-server'][param]['config']:
                required_type_name = parameters_mapping['on-server'][param].get('type', 'undefined')
                try:
                    required_type = getattr(types, required_type_name)
                except AttributeError, ex:
                    msg = "Invalid type %s specified in client configuration mapping for server parameter %s" % (required_type_name, param)
                    return False, msg
                attrs = param_full_config_name.split('.')
                obj = self.configuration
                while attrs and obj is not None:
                    obj = getattr(obj, attrs.pop(0), None)
                if obj is not None:
                    if type(obj) != required_type:
                        msg = "Crab configuration problem: Invalid type %s for parameter %s; it is needed a %s" \
                              % (str(type(obj)), param_full_config_name, str(required_type))
                        return False, msg
                else:
                    if parameters_mapping['on-server'][param].get('default') is None and parameters_mapping['on-server'][param].get('required', False):
                        msg = "Crab configuration problem: Parameter %s is missing" % param_full_config_name
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
        self.cmdconf = commands_configuration.get(self.name)
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

        ## Retrieve VO role/group from the command line options.
        proxyOptsSetPlace = {'role': '', 'group': ''}
        if self.options.voRole is not None:
            self.voRole = self.options.voRole
            proxyOptsSetPlace['role'] = 'cmdopts'
            msg = "Using VO role '%s' as specified in the command line." % (self.voRole)
            self.logger.debug(msg)
        else:
            self.voRole = ''
        if self.options.voGroup is not None:
            self.voGroup = self.options.voGroup
            proxyOptsSetPlace['group'] = 'cmdopts'
            msg = "Using VO group '%s' as specified in the command line." % (self.voGroup)
            self.logger.debug(msg)
        else:
            self.voGroup = ''
        ## Create the object that will do the proxy operations. We don't really care
        ## what VO role and group and server URL we pass to the constructor, because
        ## these are not used until we do the proxy delegation to the myproxy server.
        ## And this happens in handleProxy(), which is called after we load the
        ## configuration file and retrieve the final values for those parameters.
        ## handleProxy() takes care of passing those parameters to self.proxy.
        self.proxy = CredentialInteractions('', '', self.voRole, self.voGroup, self.logger, '')

        ## If the user didn't use the --proxy command line option, and if there isn't
        ## a valid proxy already, we create a new one with the current VO role and
        ## group (as commented above, we don't really care what are the VO role and
        ## group so far).
        self.proxy_created = False
        if not self.options.proxy and self.cmdconf['initializeProxy']:
            self.proxy_created = self.proxy.createNewVomsProxySimple(timeleftthreshold = 720)

        ## Extract the username from the proxy.
        self.proxyusername = self.proxy.getUsername()

        ## If we get an input configuration file:
        if hasattr(self.options, 'config') and self.options.config is not None:
            ## Load the configuration file and validate it.
            self.loadConfig(self.options.config, self.args)
            ## Create the CRAB project directory.
            self.requestarea, self.requestname, self.logfile = createWorkArea(self.logger, \
                                                                              getattr(self.configuration.General, 'workArea', None), \
                                                                              getattr(self.configuration.General, 'requestName', None))
            ## If VO role/group were not given in the command options, get them from
            ## the configuration file. If they are specified in both places, print a
            ## message saying that the command line options will be used.
            if hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voRole'):
                if self.options.voRole is None:
                    self.voRole = self.configuration.User.voRole
                    proxyOptsSetPlace['role'] = 'config'
                    msg = "Using VO role '%s' as specified in the configuration file." % (self.voRole)
                else:
                    msg  = "Ignoring the VO role specified in the configuration file."
                    msg += " Using VO role '%s'" % (self.voRole)
                    if self.voRole == '':
                        msg += " (i.e. no VO role)"
                    msg += " as specified in the command line."
                self.logger.debug(msg)
            if hasattr(self.configuration, 'User') and hasattr(self.configuration.User, 'voGroup'):
                if self.options.voGroup is None:
                    self.voGroup = self.configuration.User.voGroup
                    proxyOptsSetPlace['group'] = 'config'
                    msg = "Using VO group '%s' as specified in the configuration file." % (self.voGroup)
                else:
                    msg  = "Ignoring the VO group specified in the configuration file."
                    msg += " Using VO group '%s'" % (self.voGroup)
                    if self.voGroup == '':
                        msg += " (i.e. no VO group)"
                    msg += " as specified in the command line."
                self.logger.debug(msg)

        ## If we get an input task, we load the cache and set the server URL
        ## and VO role and group from it.
        if hasattr(self.options, 'task') and self.options.task:
            self.loadLocalCache(proxyOptsSetPlace)

        ## If the server URL isn't already set, we check the args and then the config.
        if not hasattr(self, 'serverurl') and self.cmdconf['requiresREST']:
            self.instance, self.serverurl = self.serverInstance()
        elif not self.cmdconf['requiresREST']:
            self.instance, self.serverurl = None, None

        ## Update (or create) the .crab3 cache file.
        self.updateCrab3()

        ## Unless the user used the --proxy command line option, at this point we
        ## are sure that there is a valid proxy (because we have already checked
        ## that, and eventually created a new one). If the user didn't use the
        ## --proxy command line option, and if there is a valid proxy not created
        ## by CRAB, we check that the VO role and group in the proxy are the same
        ## as specified by the user in the configuration file (or in the command
        ## line options). If they are not, we ask the user if he/she wants to
        ## overwrite the current proxy. If the user doesn't want to overwrite it,
        ## we don't continue and ask him/her to change the VO role and group in
        ## the configuration file (or in the command line options) to match what
        ## is in the existing proxy. The reason we do this is the following: when
        ## delegating the proxy to myproxy server, the VO role and group used in
        ## the delegation are the ones specified by the user in the configuration
        ## file (or in the command line options), and not the ones in the proxy,
        ## and we don't want that the user gets confused about what VO role and
        ## group are being used.
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

        if hasattr(self.options, 'instance') and not self.options.instance is None:
            if hasattr(self, 'configuration') and hasattr(self.configuration.General, 'instance') and not self.configuration.General.instance is None:
                self.logger.info('%sWarning%s: Instance value in configuration file is overwritten by the option command, %s intance will be use ' % (colors.RED, colors.NORMAL, self.options.instance))

            if self.options.instance in SERVICE_INSTANCES.keys():
                instance = self.options.instance
                serverurl = SERVICE_INSTANCES[instance]
            else:
                instance = 'private'
                serverurl = self.options.instance
        elif hasattr(self, 'configuration') and hasattr(self.configuration.General, 'instance') and not self.configuration.General.instance is None:
            if self.configuration.General.instance in SERVICE_INSTANCES.keys():
                instance = self.configuration.General.instance
                serverurl = SERVICE_INSTANCES[instance]
            else:
                instance = 'private'
                serverurl = self.configuration.General.instance
        else:
            instance = 'prod'
            serverurl = SERVICE_INSTANCES[instance]

        return instance, serverurl


    def checkversion(self, baseurl = None):
        compatibleversion = server_info('version', self.serverurl, self.proxyfilename, baseurl)
        if __version__ in compatibleversion:
            self.logger.debug("CRABClient version: %s Compatible"  % __version__)
        else:
            self.logger.info("%sWARNING%s: Incompatible CRABClient version \"%s\" " % (colors.RED, colors.NORMAL , __version__ ))
            self.logger.info("Server is saying that compatible versions are: %s"  % compatibleversion)


    def handleProxy(self, proxyOptsSetPlace):
        """ 
        Init the user proxy, and delegate it if necessary.
        """
        if not self.options.proxy:
            if self.cmdconf['initializeProxy']:
                self.proxy.setVOGroupVORole(self.voGroup, self.voRole)
                self.proxy.setMyProxyAccount(self.serverurl)
                _, self.proxyfilename = self.proxy.createNewVomsProxy(timeleftthreshold = 720, \
                                                                      proxyCreatedByCRAB = self.proxy_created, \
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
        self.requestarea, self.requestname = getWorkArea( self.options.task )
        self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
        port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
        self.instance = self.cachedinfo['instance']
        self.serverurl = self.cachedinfo['Server'] + port
        if self.options.voRole is None or self.options.voGroup is None:
            msgadd = []
            if self.options.voRole is None:
                self.voRole = self.cachedinfo['voRole']
                proxyOptsSetPlace['role'] = 'cache'
                msgadd.append("role '%s'" % (self.voRole))
            if self.options.voGroup is None:
                self.voGroup = self.cachedinfo['voGroup']
                proxyOptsSetPlace['group'] = 'cache'
                msgadd.append("group '%s'" % (self.voGroup))
            msg = "Using VO %s as written in the request cache file for this task." % (" and ".join(msgadd))
            self.logger.debug(msg)


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
