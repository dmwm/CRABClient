#!/usr/bin/env python
# encoding: utf-8
"""
Client_t.py
"""

from WMCore.Configuration import Configuration
import os
import unittest
import logging
import socket
from CRABClient import Handler

class ClientTest(unittest.TestCase):
    # TODO: WOrk out how to mock the server for stand alone testing
    def setUp(self):
        self.client = Handler()
        logging.basicConfig(level=logging.DEBUG)
        self.client.logger = logging.getLogger('CRAB3 client tests')

    def tearDown(self):
        """
        Standard tearDown

        """

        #os.system('rm -rf crab_%s' % testConfig.General.requestName)
        pass

    def testBadCommand(self):
        """
        Test executing a command that doesn't exist, make sure a KeyError
        is raised and that the return code is 1
        """
        commando = 'foo'
        self.client.loadConfig(testConfig)

        self.assertRaises(KeyError, self.client, commando, {})

    def testNoServer(self):
        """
        For each command that interacts with the server make sure that a
        socket error is raised when the server isn't present.
        """
        self.client.loadConfig(testConfig)
#        self.client.initialise('submit')

        self.assertRaises(socket.error, self.client, 'status', {})
        #self.assertRaises(socket.error, self.client.runCommand, 'status', {'task': []})


testConfig = Configuration()

## General options for the client
testConfig.section_("General")
testConfig.General.server_url    = 'fake.server:8080'
testConfig.General.requestName   = 'MyAnalysis'

## Specific option of the job type
## these options are directly readable from the job type plugin
testConfig.section_("JobType")
testConfig.JobType.pluginName  = 'Example'
testConfig.JobType.psetName    = 'pset.py'
testConfig.JobType.inputFiles  = ['/tmp/input_file']

## Specific data options
testConfig.section_("Data")
testConfig.Data.inputDatasetList = ['/cms/data/set']
testConfig.Data.lumiSectionFile  = '/file/path/name'

## User options
testConfig.section_("User")
testConfig.User.role = '/cms/integration'


if __name__ == "__main__":
    unittest.main()
