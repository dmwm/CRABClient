import os
from optparse import OptionParser, SUPPRESS_HELP

from CRABClient.client_utilities import loadCache, getWorkArea, initProxy

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

    _cache = {}
    def create_cached(cls, cmd):
        if cls._cache == {}:
            from CRABClient.ClientMapping import defaulturi
            cls._cache = defaulturi
        if cmd in cls._cache:
            return cls._cache[cmd]
        return None
    create_cached = classmethod(create_cached)


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

        self.parser = OptionParser(description = self.__doc__, usage = self.usage, add_help_option = True)
        ## TODO: check on self.name should be removed (creating another abstraction in between or refactoring this)
        if self.name == 'submit':
            self.parser.disable_interspersed_args()
        self.setSuperOptions()

        (self.options, self.args) = self.parser.parse_args( cmdargs )

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
        cmdmap = None

        ## if the server name is an CLI option
        if hasattr(self.options, 'server') and self.options.server is not None:
            serverurl = self.options.server
        ## but the server name can be cached in some cases
        elif hasattr(self.options, 'task') and self.options.task:
            self.requestarea, self.requestname = getWorkArea( self.options.task )
            self.cachedinfo, self.logfile = loadCache(self.requestarea, self.logger)
            serverurl = self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port'])

        ## if we have got a server url we create the cache
        if serverurl:
            cmdmap = SubCommand.create_cached(self.name)

        ## not all the commands need an uri (e.g.: remote_copy)
        if cmdmap:
            self.uri = cmdmap['uri']
            if 'map' in cmdmap:
                self.requestmapper = cmdmap['map']


    def __call__(self):
        self.logger.info("This is a 'nothing to do' command")
        raise NotImplementedException


    def terminate(self, exitcode):
        #We do not want to print logfile for each command...
        if exitcode < 2000:
            self.logger.info("Log file is %s" % os.path.abspath(self.logfile))


    def setOptions(self):
        raise NotImplementedError


    def setSuperOptions(self):
        self.parser.add_option( "-p", "--skip-proxy",
                                 dest = "skipProxy",
                                 default = False,
                                 help = "Skip Grid proxy creation and myproxy delegation",
                                 metavar = "USERDN" )

        try:
            self.setOptions()
        except NotImplementedError:
            pass


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
        """
        return True, "Valid configuration"
