import os
from optparse import OptionParser, SUPPRESS_HELP

from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_utilities import loadCache, getWorkArea


class SubCommand(object):

    ## setting visible = False doesn't allow the sub-command to be called from CLI
    visible = True
    usage = "usage: %prog [command-options] [args]"

    _cache = {}
    def create_cached(cls, cmd, crabserverurl, mappingpath):
        if cls._cache == {}:
            server = HTTPRequests( crabserverurl )
            dictresult, status, reason = server.get( mappingpath )
            if status == 200 and not dictresult == {}:
                cls._cache = dictresult
        if cmd in cls._cache:
            return cls._cache[cmd]
        return None
    create_cached = classmethod(create_cached)


    def __init__(self, logger, cmdargs = []):
        """
        Initialize common client parameters
        """
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
        ##and to figure out which server to contact
        if self.name != 'submit':
            self.createCache()


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
            self.logger.debug('Contacting ' + serverurl)
            cmdmap = SubCommand.create_cached(self.name, serverurl, self.options.mappingpath)
            self.logger.debug('Retrieved client-server mapping')

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
        self.parser.add_option( "--mapping-path",
                                dest = "mappingpath",
                                default = '/crabinterface/crab/requestmapping',
                                help = SUPPRESS_HELP)
        try:
            self.setOptions()
        except NotImplementedError:
            pass
