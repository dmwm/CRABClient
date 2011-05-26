from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
import logging
import os


databaseURL = os.getenv("DATABASE")
databaseSocket = os.getenv("DBSOCK")

couchURL = os.getenv("COUCHURL")
workloadDB = 'workload_db_test'
configCacheDB = 'config_cache_test'
jsmCacheDB = 'jsmcache_test'


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
        self.config.Webtools.port = 8588

        #DB Parameters used by RESTServerSetup
        self.config.UnitTests.views.active.rest.database.connectUrl = databaseURL
        self.config.UnitTests.views.active.rest.database.socket = databaseSocket
        #DB Parameters used by
#        self.config.UnitTests.section_('database')
#        self.config.UnitTests.database.connectUrl = databaseURL
#        self.config.UnitTests.database.socket = databaseSocket
        self.config.UnitTests.object = 'CRABRESTModelMock'
        #self.config.UnitTests.views.active.rest.logLevel = 'DEBUG'

        self.schemaModules = ['WMCore.RequestManager.RequestDB']
        self.urlbase = self.config.getServerUrl()


    def setUp(self):
        """
        _setUp_
        """
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch(workloadDB)
        self.testInit.setupCouch(configCacheDB, "ConfigCache")

        self.testInit.setupCouch(jsmCacheDB + "/fwjrs", "FWJRDump")

#        for v in allSoftwareVersions():
#            SoftwareAdmin.addSoftware(v)


    def tearDown(self):
        self.testInit.tearDownCouch()
        RESTBaseUnitTest.tearDown(self)
