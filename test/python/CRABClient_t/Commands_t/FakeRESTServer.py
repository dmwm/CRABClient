from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
import logging
import os

class FakeRESTServer(RESTBaseUnitTest):
    """
    Loads a the CRABRESTModelMock REST interface which emulates the CRABRESTModel class.
    When testing a command which requires an interaction with the server wi will contact
    this class.
    """

    def initialize(self):
        self.config = DefaultConfig('CRABRESTModelMock')
        self.config.Webtools.environment = 'development'
        self.config.Webtools.error_log_level = logging.ERROR
        self.config.Webtools.access_log_level = logging.ERROR
        self.config.Webtools.port = 8518
        self.config.Webtools.host = '127.0.0.1'
        self.config.UnitTests.object = 'CRABRESTModelMock'
        #self.config.UnitTests.views.active.rest.logLevel = 'DEBUG'

        self.urlbase = self.config.getServerUrl()


    def setUp(self):
        """
        _setUp_
        """
        RESTBaseUnitTest.setUp(self)


    def tearDown(self):
        RESTBaseUnitTest.tearDown(self)
