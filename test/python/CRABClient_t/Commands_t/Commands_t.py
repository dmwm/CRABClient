import CRABRESTModelMock
from FakeRESTServer import FakeRESTServer
from WMCore.Configuration import Configuration
from Commands.server_info import server_info
from Commands.getoutput import getoutput
from Commands.report import report
from Commands.status import status
from Commands.submit import submit
from Commands.kill import kill
from Commands.postmortem import postmortem
from Commands import CommandResult
from client_utilities import createCache, createWorkArea
from client_exceptions import TaskNotFoundException, CachefileNotFoundException

import unittest
import logging
import json
import os
import shutil
import time
import imp
from socket import error as SocketError

class CommandTest(FakeRESTServer):

    TEST_REQ_NAME = 'TestAnalysis'
    maplistopt = ["--mapping-path","/unittests/rest/requestmapping/"]

    def __init__(self, config):
        FakeRESTServer.__init__(self, config)
        self.setLog()


    def setUp(self):
        #Dynamic import of the configuration which in principle is not in the PYTHONPATH
        self.TestConfig = imp.load_source('TestConfig', os.path.join( os.path.dirname(__file__), "../../../data/TestConfig.py"))
        FakeRESTServer.setUp(self)
        self._prepareWorkArea()
        #time.sleep(1000)


    def tearDown(self):
        FakeRESTServer.tearDown(self)
        self._cleanWorkArea()
        if os.path.isfile(CRABRESTModelMock.FILE_NAME):
            os.remove(CRABRESTModelMock.FILE_NAME)
        if os.path.isdir("crab_" + self.TestConfig.config.General.requestName):
            shutil.rmtree("crab_" + self.TestConfig.config.General.requestName)


    def setLog(self):
        self.logger = logging.getLogger("CommandTest")
        self.logger.setLevel(logging.DEBUG)


    def testServerInfo(self):
        si = server_info(self.logger, self.maplistopt + ["-s","localhost:8518"])

        #1) check that the right API is called
        res = si()
        expRes = CommandResult(0, CRABRESTModelMock.SI_RESULT)
        self.assertEquals(expRes, res)

        #2) wrong -s option: the user give us an address that does not exists
        #self.assertRaises(SocketError, si)
        #, ["-s","localhost:8518"])


    def testStatus(self):
        s = status(self.logger, self.maplistopt)

        #1) missing required -t option
        expRes = CommandResult(1, 'ERROR: Task option is required')
        res = s()
        self.assertEquals(expRes, res)

        #2) correct execution
        analysisDir = self.reqarea
        s = status(self.logger, self.maplistopt + ["-t", analysisDir])
        res = s()
        expRes = CommandResult(0, None)
        self.assertEquals( expRes, res)

        #3) Print request details
        s = status(self.logger, self.maplistopt + ["-t", analysisDir])
        s._printRequestDetails({u'requestDetails': {u'RequestMessages': [[u'No blocks pass white/blacklist']], 'RequestStatus': 'failed'}})

        #4) .requestcache file does note exists
        os.remove(os.path.join(analysisDir, ".requestcache"))
        self.assertRaises( CachefileNotFoundException, status, self.logger, self.maplistopt + ["-t", analysisDir])

        #5) wrong -t option
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_XXX')
        self.assertRaises( TaskNotFoundException, status, self.logger, self.maplistopt + ["-t", analysisDir])



    def testReport(self):
        """
        Test the functionality of the report command
        """

        # Missing required -t option
        expRes = CommandResult(1, 'ERROR: Task option is required')
        rep = report(self.logger, [])
        res = rep()
        self.assertEquals(expRes, res)

        # Executes
        analysisDir = self.reqarea#os.path.join(os.path.dirname(__file__), 'crab_AnalysisName')
        rep = report(self.logger, self.maplistopt + ["-t", analysisDir])
        res = rep()
        expRes = CommandResult(0, None)
        self.assertEquals(expRes, res)

        lumiReportFilename = os.path.join("crab_"+self.TEST_REQ_NAME, "results", "lumiReport.json")

        # Wrote correct file
        with open(lumiReportFilename, 'r') as reportFile:
            result = json.load(reportFile)
            mockResult = json.loads(CRABRESTModelMock.goodLumisResult)
            self.assertEquals(result, mockResult)

        os.remove(lumiReportFilename)


    def testGetOutput(self):
        """
        Crete a fake source output file and verify it is copied to the correct
        dest directory
        """
        #f = open("src_output.root", 'w')
        #f.close()

        #1) missing required -t option (the other required option, -r, is ignored)
        go = getoutput(self.logger, self.maplistopt)
        res = go()
        expRes = CommandResult(1, 'ERROR: Task option is required')

        #2) -t option is present but -r is missing
        analysisDir = self.reqarea
        go = getoutput(self.logger, self.maplistopt + ["-t", analysisDir])
        res = go()
        expRes = CommandResult(1, 'ERROR: Range option is required')

        #3) request passed with the -t option does not exist
        #res = go(["-t", analysisDir + "asdf"])
        #TODO we expect an appropriate answer from the server.
        #By now, the server just answer an ampty list

        #4) check correct behaviour without specifying output directory
        #N.B.: -p options is required for tests to skip proxy creation and delegation
        go = getoutput(self.logger, self.maplistopt + ["-t", analysisDir, "-r", "20", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        destDir = os.path.join(analysisDir, 'results')
        self.assertTrue(os.path.isdir(destDir))
        self.assertTrue(os.path.isfile(os.path.join(destDir, '20.root')))
        #Remove the directory
        shutil.rmtree(destDir)
        self.assertFalse(os.path.isdir(destDir))

        #5) correct behavior and output directory specified which exists
        go = getoutput(self.logger, self.maplistopt + ["-t", analysisDir, "-r", "20", "-o", "/tmp", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        self.assertTrue(os.path.isdir('/tmp'))
        destFile = os.path.join('/tmp', '20.root')
        self.assertTrue(os.path.isfile(destFile))
        os.remove(destFile)
        self.assertFalse(os.path.isfile(destFile))

        #6) correct behavior and output directory specified which does not exists
        go = getoutput(self.logger, self.maplistopt + ["-t", analysisDir, "-r", "20", "-o", "/tmp/asdf/qwerty", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        self.assertTrue(os.path.isdir('/tmp/asdf/qwerty'))
        #Remove the directory
        shutil.rmtree('/tmp/asdf/qwerty')


    def testSubmit(self):
        #Delete workdir
        if os.path.isdir("crab_" + self.TestConfig.config.General.requestName):
            shutil.rmtree("crab_" + self.TestConfig.config.General.requestName)

        #2) The config file is not found
        sub = submit(self.logger, self.maplistopt + ["-c", "asdf", "-p", "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni", \
                                       "-s", "127.0.0.1:8518"])
        res = sub()
        self.assertEqual(res[0], 1)

        #3) Is the client chacking the configurations?
        #If a mandatory section is not there => fail!
        sections = ["General", "User", "Data", "Site" , "JobType"]#mandatory sections
        emptyConf = Configuration()
        for sec in sections:
            sub = submit(self.logger, self.maplistopt + ["-c", "asdf", "-p", "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni", \
                                           "-s", "127.0.0.1:8518"])
            res = sub()
            self.assertEqual(res[0], 1)
            emptyConf.section_(sec)

        emptyConf.General.serverUrl = "localhost:8518"
        emptyConf.JobType.externalPluginFile = os.path.join( os.path.dirname(__file__), "TestPlugin.py")
        emptyConf.Site.storageSite = 'T2_XXX'
        expRes = CommandResult(0, None)
        sub = submit(self.logger, self.maplistopt + ["-c", emptyConf, "-p", "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni", \
                                       "-s", "127.0.0.1:8518"])
        res = sub()
        self.assertEquals(res, expRes)


    def testLumiSubmit(self):
        """
        Test submission with the lumiMask parameter"
        """

        sections = ["General", "User", "Data", "Site" , "JobType"]
        lumiConf = Configuration()
        for sec in sections:
            lumiConf.section_(sec)

        lumiConf.General.serverUrl = "localhost:8518"
        lumiConf.JobType.externalPluginFile = os.path.join( os.path.dirname(__file__), "TestPlugin.py")
        lumiConf.Site.storageSite = 'T2_XXX'

        lumiInput = os.path.join( os.path.dirname(__file__), "../../../data/lumiInput.json")
        lumiConf.Data.splitting = 'LumiBased'
        lumiConf.Data.lumiMask = 'lumiInput.json'

        sub = submit(self.logger, self.maplistopt + ["-c", lumiConf,
                                                     "-p", "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni",
                                                     "-s", "127.0.0.1:8518"])

        res = sub()
        expRes = CommandResult(0, None)
        self.assertEquals(res, expRes)

    def testPostMortem(self):
        s = postmortem(self.logger, self.maplistopt)

        #1) missing required -t option
        expRes = CommandResult(1, 'ERROR: Task option is required')
        res = s()
        self.assertEquals(expRes, res)

        #2) correct execution
        analysisDir = self.reqarea
        s = postmortem(self.logger, self.maplistopt + ["-t", analysisDir])
        res = s()
        expRes = CommandResult(0, None)
        self.assertEquals( expRes, res)

        #3) wrong -t option
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_XXX')
        self.assertRaises( TaskNotFoundException, postmortem, self.logger, self.maplistopt + ["-t", analysisDir])

    def testKill(self):
        s = kill(self.logger, [])

        #1) missing required -t option
        expRes = CommandResult(1, 'ERROR: Task option is required')
        res = s()
        self.assertEquals(expRes, res)

        #2) correct execution
        analysisDir = self.reqarea
        s = kill(self.logger, self.maplistopt + ["-t", analysisDir])
        res = s()
        expRes = CommandResult(0, None)
        self.assertEquals( expRes, res)

        #3) wrong -t option
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_XXX')
        self.assertRaises( TaskNotFoundException, kill, self.logger, self.maplistopt + ["-t", analysisDir])


    def _prepareWorkArea(self):
        self.reqarea, self.reqname = createWorkArea(self.logger, requestName = self.TEST_REQ_NAME)
        server = {}
        server['conn'] = TestServerParam("127.0.0.1", 8518)
        createCache(self.reqarea, server, self.reqname)


    def _cleanWorkArea(self):
        shutil.rmtree(self.reqarea)


class TestServerParam:
    def __init__(self, host, port):
        self.host = host
        self.port = port




if __name__ == "__main__":
    unittest.main()
