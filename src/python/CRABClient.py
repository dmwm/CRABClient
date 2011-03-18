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

        msg = 'Detailed query of tasks specified, can query a list'
        statusoptions = Options('task', 't', msg, None, "append")

        self.commands['status'] = {'function': status, 'options': [statusoptions]}

        self.commands['serverstatus'] = {'function': server_info, 'options': []}

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

    def initialise(self, cmd, opt):
        """
        Contact the server, get its configuration e.g. MyProxy server, local SE.
        then set up a handler for dealing with credentials
        """
        url = self.configuration.General.server_url
        self.server = HTTPRequests(url)
        serverdn = self.runCommand('serverstatus')[1]['server_dn']
        self.configuration.General.serverdn = serverdn
        if cmd in ['submit']:
            self.requestarea, \
            self.requestname = createWorkArea( self.logger,
                                               getattr(self.configuration.General, 'workArea', None),
                                               getattr(self.configuration.General, 'requestName', None)
                                             )
            self.logger.debug("Working on %s" % str(self.requestarea))
        else:
            if opt is not None:
                if 'task' in opt.keys():
                    self.requestarea = opt['task'][0]
                    self.requestname = os.path.split(os.path.normpath(self.requestarea))[1]

    def __call__(self, command, commandoptions=None):
        """
        Initialise the client, run the command and return the exit code
        """
        self.initialise(command, commandoptions)

        self.logger.info("Request name: %s " % str(self.requestname) )
        self.logger.info("Working area: %s " % str(self.requestarea) )
        exitcode, data = self.runCommand(command, commandoptions)
        return exitcode

    def runCommand(self, command, commandoptions=None):
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
