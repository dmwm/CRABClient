"""
The client class
"""

from collections import namedtuple
from WMCore.Configuration import loadConfigurationFile, Configuration

# ServerInteractions should probably go and be replaced by WMCore's JSON Request
from ServerInteractions import HTTPRequests
from client_utilities import createWorkArea

import os
import json

# I'd like to flatten this to a module of functions...
from Commands import *

__version__ = "3.0.1"

class Handler(object):
    """
    This is the first prototype version of CRAB-3 client.
    """

    def __init__(self):
        self.logger = None
        self.configuration = None
        self.server = None
        self.requestarea = None
        self.requestname = None
        # Make a dictionary of commands available to the client;
        # function is the function/method to call, options is a list of
        # namedtuples like (option_name, description, [default, [action]])
        # and are used to generate the help
        Options = namedtuple('Options', 'name, short_name, help, default, action')

        self.commands = {}

        # submit_options = Options('foo', None, 'bar', 123, None)
        self.commands['jobtypes'] = {'function': job_types, 'options': []}

        self.commands['submit'] = {'function': submit, 'options': []}

        self.commands['status'] = {'function': status, 'options': []}

        self.commands['serverinfo'] = {'function': server_info, 'options': []}

        msg = 'Retrieving the job output directly from the storage element, it takes a list or range as input'
        rangoutputoptions = Options('range', 'r', msg, None, 'append')
        msg = 'Where the output files retrieved will be stored in the local file system'
        destoutputoptions = Options('outputpath', 'o', msg, None, 'append')
        self.commands['getoutput'] = {'function': getoutput, 'options': [rangoutputoptions, destoutputoptions]}

    def version(self):
        """
        Return the version of the client
        """
        return __version__

    def setLogger(self, logger):
        """
        Set the logger the client instance should use
        """
        self.logger = logger

    def loadConfig(self, config):
        """
        Load the configuration file
        """
        self.logger.debug('Loading configuration')
        if type(config) == Configuration:
            self.configuration = config
        else:
            self.configuration = loadConfigurationFile(config)
        # TODO: Validate self.configuration here

    def initialise(self, cmd, opt, task):
        """
        Contact the server, get its configuration e.g. MyProxy server, local SE.
        then set up a handler for dealing with credentials
        """
        url = self.configuration.General.server_url
        self.server = HTTPRequests(url)
        serverinfo = self.runCommand('serverinfo')[1]
        self.configuration.General.serverdn   = serverinfo['server_dn']
        self.configuration.General.myproxy    = serverinfo['my_proxy']
        self.configuration.General.sbservhost = serverinfo['sandbox']['endpoint']
        self.configuration.General.sbservport = serverinfo['sandbox']['port']
        self.configuration.General.sbservtype = serverinfo['sandbox']['type']
        self.configuration.General.sbservpath = serverinfo['sandbox']['basepath']
        if cmd in ['submit']:
            self.requestarea, \
            self.requestname = createWorkArea( self.logger,
                                               getattr(self.configuration.General, 'workArea', None),
                                               getattr(self.configuration.General, 'requestName', None)
                                             )
            self.logger.debug("Working on %s" % str(self.requestarea))
        else:
            if task is not None:
                if os.path.isabs( task ):
                    self.requestarea = task
                    self.requestname = os.path.split(os.path.normpath(self.requestarea))[1]
                else:
                    self.requestarea = os.path.abspath( task )
                    self.requestname = task

    def __call__(self, command, commandoptions = None, task = None):
        """
        Initialise the client, run the command and return the exit code
        """
        self.initialise(command, commandoptions, task)

        self.logger.info("Request name: %s " % str(self.requestname) )
        self.logger.info("Working area: %s " % str(self.requestarea) )
        exitcode, data = self.runCommand(command, commandoptions)
        self.logger.info( str(data) )
        return exitcode

    def runCommand(self, command, commandoptions = None):
        """
        Execute the command specified and return the exitcode. Exceptions are handled
        by whatever calls the class - this is a library not a client.

        Command functions are given a logger, the user configuration an instance
        of ServerInteractions an instance of CredentialInteractions and the opts
        for the command.
        """
        self.logger.debug('Running %s' % command)
        exitcode, data =  self.commands[command]['function'](self.logger,
                                                  self.configuration,
                                                  self.server,
                                                  commandoptions,
                                                  self.requestname,
                                                  self.requestarea
                                                 )

        return exitcode, data
