import os
import imp
from optparse import OptionParser, SUPPRESS_HELP

from CRABClient.client_utilities import loadCache, getWorkArea, delegateProxy, initProxy, server_info, validServerURL, createWorkArea
from CRABClient.client_exceptions import ConfigurationException, MissingOptionException
from CRABClient.ClientMapping import mapping

from WMCore.Configuration import loadConfigurationFile, Configuration

BASEURL = '/crabserver/'
SERVICE_INSTANCES = {'prod': 'cmsweb.cern.ch',
                     'preprod': 'cmsweb-testbed.cern.ch',
                     'dev': 'cmsweb-dev.cern.ch',
                     'private': None,}

class ConfigCommand:
    """ Commands which needs to load the configuration file (e.g.: submit, publish) must subclass ConfigCommand
        Provides methods for loading the configuration file handling the errors
    """

    def loadConfig(self, configname, overrideargs=None):
        """
        Load the configuration file
        """

        if not os.path.isfile(configname):
            raise ConfigurationException("Configuration file '%s' not found" % configname)

        try:
            self.logger.debug('Loading configuration')
            self.configuration = loadConfigurationFile( os.path.abspath(configname))
            if overrideargs:
                for singlearg in overrideargs:
                    fullparname, parval = singlearg.split('=')
                    # now supporting just one sub params, eg: Data.inputFiles, User.email, ...
                    parnames = fullparname.split('.', 1)
                    self.configuration.section_(parnames[0])
                    setattr(getattr(self.configuration, parnames[0]), parnames[1], parval)
                    self.logger.debug('Overriden parameter %s with %s' % (fullparname, parval))
            valid, configmsg = self.validateConfig() #subclasses of SubCommand overrhide this if needed
        except RuntimeError, re:
            msg = self._extractReason(configname, re)
            raise ConfigurationException("Configuration syntax error: \n %s.\nSee the crab.log file for more details" % msg)
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

        #workarea has not been created yet
        with open('crab.log', 'w') as of:
            of.write(str(re))

        return msg

    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there
        Not all the commands requires a configuration
        """
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
            if instance == 'private':
                instance = 'dev'
            return BASEURL + instance + '/' + resource
        raise ConfigurationException('Error: only %s instances can be used.' %str(SERVICE_INSTANCES.keys()))

    def loadMapping(self):
        """
        Load the command mapping from ClientMapping
        """
        #XXX Isn't it better to just copy the mapping with self.mapping = mapping[self.name] instead of copying each parameter?
        if self.name in mapping:
            if 'map' in mapping[self.name]:
                self.requestmapper = mapping[self.name]['map']
            self.requiresTaskOption = 'requiresTaskOption' in mapping[self.name] and mapping[self.name]['requiresTaskOption']
            self.initializeProxy = True if 'initializeProxy' not in mapping[self.name] else mapping[self.name]['initializeProxy']
            if 'other-config-params' in mapping[self.name]:
                self.otherConfigParams = mapping[self.name]['other-config-params']

    def __init__(self, logger, cmdargs = []):
        """
        Initialize common client parameters
        """
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__
        self.usage = "usage: %prog " + self.name + " [options] [args]"
        self.logger = logger
        self.logfile = ''
        self.logger.debug("Executing command: '%s'" % str(self.name))

        ##Get the mapping
        self.loadMapping()

        self.parser = OptionParser(description = self.__doc__, usage = self.usage, add_help_option = True)
        ## TODO: check on self.name should be removed (creating another abstraction in between or refactoring this)
        if self.name == 'submit':
            self.parser.disable_interspersed_args()
        self.setSuperOptions()

        ##Parse the command line parameters
        (self.options, self.args) = self.parser.parse_args( cmdargs )

        ##Validate the command line parameters before initing the proxy
        self.validateOptions()

        ##if we get an input configuration we load it
        if hasattr(self.options, 'config') and self.options.config is not None:
            self.loadConfig( self.options.config, self.args )
            self.requestarea, self.requestname, self.logfile = createWorkArea(self.logger,
                                                                              getattr(self.configuration.General, 'workArea', None),
                                                                              getattr(self.configuration.General, 'requestName', None))
            self.voRole = self.options.voRole if not self.options.voRole else getattr(self.configuration.User, "voRole", "")
            self.voGroup = self.options.voGroup if not self.options.voGroup else getattr(self.configuration.User, "voGroup", "")

        ##if we get an input task we load the cache and set the url from it
        if hasattr(self.options, 'task') and self.options.task:
            self.createCache()

        ## if the server url isn't already set we check the args and then the config
        if not hasattr(self, 'serverurl'):
            self.instance, self.serverurl = self.serverInstance()

        self.handleProxy(self.getUrl(self.instance, resource='info'))
        self.uri = self.getUrl(self.instance)
        self.logger.debug("Instance is %s" %(self.instance))
        self.logger.debug("Server base url is %s" %(self.serverurl))
        self.logger.debug("Command url %s" %(self.uri))

    def serverInstance(self):
        """Deriving the correct instance to use and the server url"""
        serverurl = None
        instance = 'prod'
        if hasattr(self.options, 'instance') and self.options.instance is not None and self.options.instance in SERVICE_INSTANCES:
            instance = self.options.instance
        elif hasattr(self.configuration.General, 'instance') and self.configuration.General.instance is not None and self.configuration.General.instance in SERVICE_INSTANCES:
            instance = self.configuration.General.instance
        if SERVICE_INSTANCES[instance] is None:
            if getattr(self.options, 'server', None) is not None:
                serverurl = self.options.server
            elif getattr(self.configuration.General, 'serverUrl') is not None:
                serverurl = self.configuration.General.serverUrl
        else: 
            serverurl = SERVICE_INSTANCES[instance]
        if instance is not None and serverurl is not None:
            return instance, serverurl
        raise ConfigurationException("No correct instance or no server specified")

    def handleProxy(self, baseurl=None):
        """ Init the user proxy, and delegate it if necessary.
        """
        if not self.options.skipProxy and self.initializeProxy:
            _, self.proxyfilename, proxyobj = initProxy( self.voRole, self.voGroup, self.logger )
            #get the dn of the agents from the server
            alldns = server_info('delegatedn', self.serverurl, self.proxyfilename, baseurl)
            #for each agentDN received from the server, delegate it!
            #XXX Temporary solution. Need to figure out how to delegate credential to the several WMAgent
            #without forcing the user to insert the password several times
            if 'rest' in alldns and alldns['rest']:
                delegateProxy(alldns['rest'], 'myproxy.cern.ch', proxyobj, self.logger, nokey=True)
            if 'services' in alldns:
                for serverdn in alldns['services']:
                    delegateProxy(serverdn, 'myproxy.cern.ch', proxyobj, self.logger, nokey=False)
        else:
            self.proxyfilename = self.options.skipProxy
            self.logger.debug('Skipping proxy creation')

    def createCache(self, serverurl = None):
        """ Loads the client cache and set up the server url
        """
        self.requestarea, self.requestname = getWorkArea( self.options.task )
        self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
        port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
        self.instance = self.cachedinfo['instance']
        self.serverurl = self.cachedinfo['Server'] + port
        self.voRole = self.cachedinfo['voRole'] if not self.options.voRole else self.options.voRole
        self.voGroup = self.cachedinfo['voGroup'] if not self.options.voGroup else self.options.voGroup

    def __call__(self):
        self.logger.info("This is a 'nothing to do' command")
        raise NotImplementedError

    def terminate(self, exitcode):
        #We do not want to print logfile for each command...
        if exitcode < 2000:
            self.logger.info("Log file is %s" % os.path.abspath(self.logfile))

    def setOptions(self):
        raise NotImplementedError

    def setSuperOptions(self):
        try:
            self.setOptions()
        except NotImplementedError:
            pass

        self.parser.add_option( "-p", "--skip-proxy",
                                 dest = "skipProxy",
                                 default = False,
                                 help = "Skip Grid proxy creation and myproxy delegation",
                                 metavar = "USERDN" )

        if self.requiresTaskOption:
            self.parser.add_option( "-t", "--task",
                                     dest = "task",
                                     default = None,
                                     help = "Same as -c/-continue" )

        self.parser.add_option( "-r", "--voRole",
                                dest = "voRole",
                                default = '' )

        self.parser.add_option( "-g", "--voGroup",
                                dest = "voGroup",
                                default = '' )

        self.parser.add_option("-i", "--instance",
                               dest = "instance",
                               type = "string",
                               help = "Running instance of CRAB service. Valid values are %s" %str(SERVICE_INSTANCES.keys()))

        self.parser.add_option( "-s", "--server",
                                 dest = "server",
                                 action = "callback",
                                 type   = 'str',
                                 nargs  = 1,
                                 callback = validServerURL,
                                 metavar = "http://HOSTNAME:PORT",
                                 default = None,
                                 help = "Endpoint server url to use" )

    def validateOptions(self):
        """
        __validateOptions__

        Validate the command line options of the command
        Raise a ConfigurationException in case of error, does not do anything if ok
        """
        if self.requiresTaskOption and not self.options.task:
            raise MissingOptionException('ERROR: Task option is required')
