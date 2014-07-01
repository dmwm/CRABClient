import os
import imp
import json
from optparse import OptionParser, SUPPRESS_HELP

from CRABClient.client_utilities import loadCache, getWorkArea, server_info, validServerURL, createWorkArea
from CRABClient.client_exceptions import ConfigurationException, MissingOptionException
from CRABClient.ClientMapping import mapping
from CRABClient.CredentialInteractions import CredentialInteractions
from CRABClient.__init__ import __version__
from CRABClient.client_utilities import colors
from WMCore.Credential.Proxy import Proxy

from WMCore.Configuration import loadConfigurationFile, Configuration

BASEURL = '/crabserver/'
SERVICE_INSTANCES = {'prod': 'cmsweb.cern.ch',
                     'preprod': 'cmsweb-testbed.cern.ch',
                     'dev': 'cmsweb-dev.cern.ch',
                     'private': None,}
#if certificates in myproxy expires in less than RENEW_MYPROXY_THRESHOLD days renew them
RENEW_MYPROXY_THRESHOLD = 15

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
            raise ConfigurationException("Configuration syntax error: \n %s.\nSee the ./crab.log file for more details" % msg)
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
            self.requiresREST = True if 'requiresREST' not in mapping[self.name] else mapping[self.name]['requiresREST']
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
        self.proxy = None

        ##Get the mapping
        self.loadMapping()
        self.crab3dic=self.getConfiDict()

        self.parser = OptionParser(description = self.__doc__, usage = self.usage, add_help_option = True)
        ## TODO: check on self.name should be removed (creating another abstraction in between or refactoring this)
        if self.name == 'submit':
            self.parser.disable_interspersed_args()
        self.setSuperOptions()

        ##Parse the command line parameters
        (self.options, self.args) = self.parser.parse_args( cmdargs )

        ##Validate the command line parameters before initing the proxy
        self.validateOptions()

        self.voRole  = self.options.voRole  if self.options.voRole  is not None else ''
        self.voGroup = self.options.voGroup if self.options.voGroup is not None else ''
        ##if we get an input configuration we load it
        if hasattr(self.options, 'config') and self.options.config is not None:
            self.loadConfig( self.options.config, self.args )
            self.requestarea, self.requestname, self.logfile = createWorkArea(self.logger,
                                                                              getattr(self.configuration.General, 'workArea', None),
                                                                              getattr(self.configuration.General, 'requestName', None))
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
            if getattr(self.configuration.Data, 'publication', None) and \
               not getattr(self.configuration.General, 'transferOutput', self.requestmapper['saveoutput']['default']):
                msg = "Crab configuration problem: Data.publication is ON, but General.transferOutput is OFF.\n"
                msg += "Publication can not be performed if the output files are not transferred to a permanent storage."
                self.logger.info(msg)
                raise ConfigurationException("Incompatible configuration options Data.publication and General.transferOutput")

        ##if we get an input task we load the cache and set the url from it

        if hasattr(self.options, 'task') and self.options.task:
            self.loadLocalCache()

        ## if the server url isn't already set we check the args and then the config
        if not hasattr(self, 'serverurl') and self.requiresREST:
            self.instance, self.serverurl = self.serverInstance()
        elif not self.requiresREST:
            self.instance, self.serverurl = None, None

        self.updateCrab3()
        self.handleProxy()
        #logging user command and optin use for debuging purpose
        self.logger.debug('Command use: %s' % self.name)
        self.logger.debug('Options use: %s' % cmdargs)
        if self.requiresREST:
            self.checkversion(self.getUrl(self.instance, resource='info'))
            self.uri = self.getUrl(self.instance)
        self.logger.debug("Instance is %s" %(self.instance))
        self.logger.debug("Server base url is %s" %(self.serverurl))
        if self.requiresREST:
            self.logger.debug("Command url %s" %(self.uri))

    def serverInstance(self):
        """Deriving the correct instance to use and the server url. Client is allow to propegate the instance name and coresponding url
	   via crabconfig.py or crab option --intance and --serverurl. The variable pass via crab option will always be use over the varible
	   in crabconfig.py. Instance name must be same as the one in SERVICE_INSTANCES or exception will be raise."""
        serverurl = None

        #Will be use to print available instances
        availableinstace=', '.join(SERVICE_INSTANCES)

        #if client use the --instance options
        if hasattr(self.options,'instance') and self.options.instance is not None :
            instance = self.options.instance
            #checking validity of the instance

            #for instances other than private
            if instance in SERVICE_INSTANCES and SERVICE_INSTANCES[instance] is not None:
                #giving message for user for use instance other than private but still supply a url
                if hasattr(self.options,'server') and self.options.server is not None:
                    self.logger.info("Please use 'private' instance to specify custom server url")
                    raise ConfigurationException("Custom url is given for instance other than 'private'")
                else:
                    serverurl = SERVICE_INSTANCES[instance]

            #for private instance
            elif instance in SERVICE_INSTANCES and SERVICE_INSTANCES[instance] == None:
                #checking if url is given from option
                if hasattr(self.options, 'server') and self.options.server is not None:
                    serverurl = self.options.server
                #checking if url is given from configuration
                elif hasattr(self.configuration.General, 'serverUrl'):
                    serverurl = self.configuration.General.serverUrl
                else:
                    raise MissingOptionException("Custom url is not given for private instance")

            elif instance not in SERVICE_INSTANCES:
                self.logger.info("%sError%s: Wrong instance is given, available instance : %s"  %(colors.RED, colors.NORMAL , availableinstace))
                raise ConfigurationException("Wrong instance is given in option")
            else:
                self.logger.info("%sError%s: Unknown instance option or configuration states"  %(colors.RED, colors.NORMAL))
                raise Exception

        #for client that uses the --server option but no --instance option
        elif hasattr(self.options, 'server') and self.options.server is not None:
            if hasattr(self.configuration.General, 'instance') and self.configuration.General.instance == 'private':
                serverurl = self.options.server
                instance = 'private'

            elif hasattr(self.configuration.General, 'instance') and self.configuration.General.instance != 'private':
                self.logger.info("%sError%s: Custom server url is given for instance other that 'private'" %(colors.RED, colors.NORMAL))
                raise ConfigurationException("Wrong instance is given")
            else:
                self.logger.info("%sError%s: Unknown instance option or configuration states" %(colors.RED, colors.NORMAL))
                raise Exception

        #for client that does not use --server option or --instance option
        elif hasattr(self.configuration.General, 'instance'):
            instance = self.configuration.General.instance

            #checking if instance given is vaild
            if instance not in SERVICE_INSTANCES:
                self.logger.info ("%sError%s: Wrong instance is given, available instance : %s" %(colors.RED, colors.NORMAL, availableinstace))
                raise ConfigurationException("Wrong instance is given in configuration file")

            #for instance 'private'
            elif SERVICE_INSTANCES[instance] == None:
                if hasattr(self.configuration.General, 'serverUrl'):
                    serverurl = self.configuration.General.serverUrl
                else:
                    raise ConfigurationException("%sError%s: Private instance require server url in the configuration file" %(colors.RED, colors.NORMAL))
            #for instance other than 'private'
            else:
                serverurl = SERVICE_INSTANCES[instance]
        #for client who does not give any instance option or configuration
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

    def handleProxy(self ):
        """ Init the user proxy, and delegate it if necessary.
        """

        if not self.options.skipProxy and self.initializeProxy:
            proxy = CredentialInteractions('', '', self.voRole, self.voGroup, self.logger, myproxyAccount=self.serverurl)

            self.proxy = proxy

            self.logger.debug("Checking credentials")
            _, self.proxyfilename = proxy.createNewVomsProxy( timeleftthreshold = 720 )

            if self.requiresREST: #if the command does not contact the REST we can't delegate the proxy
                proxy.myproxyAccount = self.serverurl
                baseurl = self.getUrl(self.instance, resource='info')
                #get the dn of the task workers from the server
                alldns = server_info('delegatedn', self.serverurl, self.proxyfilename, baseurl)

                for serverdn in alldns['services']:
                    proxy.defaultDelegation['serverDN'] = serverdn
                    proxy.defaultDelegation['myProxySvr'] = 'myproxy.cern.ch'

                    self.logger.debug("Registering user credentials for server %s" % serverdn)
                    proxy.createNewMyProxy( timeleftthreshold = 60 * 60 * 24 * RENEW_MYPROXY_THRESHOLD, nokey=True)
        else:
            self.proxyfilename = self.options.skipProxy
            os.environ['X509_USER_PROXY'] = self.options.skipProxy
            self.logger.debug('Skipping proxy creation')

    def loadLocalCache(self, serverurl = None):
        """ Loads the client cache and set up the server url
        """
        self.requestarea, self.requestname = getWorkArea( self.options.task )
        self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
        port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
        self.instance = self.cachedinfo['instance']
        self.serverurl = self.cachedinfo['Server'] + port
        self.voRole = self.cachedinfo['voRole'] #if not self.options.voRole else self.options.voRole
        self.voGroup = self.cachedinfo['voGroup'] #if not self.options.voGroup else self.options.voGroup

    def getConfiDict(self):
        self.logger.debug("Checking .crab3 file")
        homedir = str(os.path.expanduser('~'))
        crab3fdir = homedir + '/.crab3'
        if not os.path.isfile(crab3fdir):
            self.logger.debug("Could not find %s creating a new one" % crab3fdir)
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
                self.logger.info('%sError%s: Error in reading json file, try to do "rm -rf ~/.crab3", and do the crab comment again'\
                    % (colors.RED, colors.NORMAL))
                raise ConfigurationException
            return configdict


    def updateCrab3(self):
        if self.requiresTaskOption or hasattr(self,'requestname') and self.requestname != None:
            crab3fdir = str(os.path.expanduser('~')) + "/.crab3"
            crab3f = open(crab3fdir, 'w')
            self.crab3dic['taskname'] = self.requestarea
            json.dump(self.crab3dic, crab3f)
            crab3f.close()

    def __call__(self):
        self.logger.info("This is a 'nothing to do' command")
        raise NotImplementedError

    def terminate(self, exitcode):
        #We do not want to print logfile for each command...
        if exitcode < 2000 and not self.name == 'checkwrite' :
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
                                default = None )

        self.parser.add_option( "-g", "--voGroup",
                                dest = "voGroup",
                                default = None )
        if self.requiresREST:

            self.parser.add_option("--instance",
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

        if self.name == 'submit' and len(self.args) == 1 and self.args[0]:
            self.options.config = self.args[0]
            del self.args[:]

        if self.requiresTaskOption and not self.options.task:
            if len(self.args) == 1 and self.args[0]:
                self.options.task = self.args[0]

            elif self.name != "kill" and self.name != 'purge' and self.crab3dic["taskname"] != None:
                self.options.task = self.crab3dic["taskname"]

            else:
                raise MissingOptionException('ERROR: Task option is required')
