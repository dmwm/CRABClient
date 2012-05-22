import os
import imp
from optparse import OptionParser, SUPPRESS_HELP

from CRABClient.client_utilities import loadCache, getWorkArea, initProxy
from CRABClient.client_exceptions import ConfigurationException, MissingOptionException
from CRABClient.ClientMapping import mapping

from WMCore.Configuration import loadConfigurationFile, Configuration


class SubCommand(object):

    ####### These options can be overrhidden if needed ########
    ## setting visible = False doesn't allow the sub-command to be called from CLI
    visible = True
    proxyfilename = None
    shortnames = []
    usage = "usage: %prog [command-options] [args]"
    #Default command name is the name of the command class, but it is also possible to set the name attribute in the subclass
    #if the command name does not correspond to the class name (getlog => get-log)

    def loadMapping(self):
        """
        Load the command mapping from ClientMapping
        """
        #XXX Isn't it better to just copy the mapping with self.mapping = mapping[self.name] instead of copying each parameter?
        if self.name in mapping:
            self.uri = mapping[self.name]['uri']
            if 'map' in mapping[self.name]:
                self.requestmapper = mapping[self.name]['map']
            self.requiresTaskOption = 'requiresTaskOption' in mapping[self.name] and mapping[self.name]['requiresTaskOption']
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

        ##The submit command handles this stuff later because it needs to load the config
        ##and to figure out which server to contact.
        if self.name != 'submit':
            self.createCache()
            #the submit does this later because it can set
            if not self.options.skipProxy:
                _, self.proxyfilename = initProxy( '', '', self.logger)
            else:
                self.logger.debug('Skipping proxy creation')


    def createCache(self, serverurl = None):
        """ Loads the client cache and set up the server url
        """
        ## if the server name is an CLI option
        if hasattr(self.options, 'server') and self.options.server is not None:
            self.serverurl = self.options.server
        ## but the server name can be cached in some cases
        elif hasattr(self.options, 'task') and self.options.task:
            self.requestarea, self.requestname = getWorkArea( self.options.task )
            self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
            port = ':' + self.cachedinfo['Port'] if self.cachedinfo['Port'] else ''
            self.serverurl = self.cachedinfo['Server'] + port


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



    def validateOptions(self):
        """
        __validateOptions__

        Validate the command line options of the command
        Raise a ConfigurationException in case of error, does not do anything if ok
        """
        if self.requiresTaskOption and not self.options.task:
            raise MissingOptionException('ERROR: Task option is required')

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
