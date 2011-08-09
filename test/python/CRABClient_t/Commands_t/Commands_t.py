import CRABRESTModelMock
from FakeRESTServer import FakeRESTServer
from Commands.server_info import server_info
from Commands.getoutput import getoutput
from Commands.report import report
from Commands.status import status
from Commands import CommandResult
import client_default

import unittest
import logging
import json
import os
import shutil
from socket import error as SocketError

class CommandTest(FakeRESTServer):

    def __init__(self, config):
        FakeRESTServer.__init__(self, config)
        self.setLog()

        client_default.defaulturi = {
            'submit' :    {'uri': '/unittests/rest/task/'},
            'getlog' :    {'uri': '/unittests/rest/log/'},
            'getoutput' : {'uri': '/unittests/rest/data/'},
            'reg_user' :  {'uri': '/unittests/rest/user/'},
            'server_info' : {'uri': '/unittests/rest/info/'},
            'status' :    {'uri': '/unittests/rest/task/'},
            'report' :    {'uri': '/unittests/rest/goodLumis/'},
            'get_client_mapping': {'uri': '/unittests/rest/requestmapping/'},
        }


    def setLog(self):
        self.logger = logging.getLogger("CommandTest")
        self.logger.setLevel(logging.DEBUG)


    def testServerInfo(self):
        si = server_info(self.logger, ["-s","localhost:8518"])

        #1) check that the right API is called
        res = si()
        expRes = CommandResult(0, CRABRESTModelMock.SI_RESULT)
        self.assertEquals(expRes, res)

        #2) wrong -s option: the user give us an address that does not exists
        #self.assertRaises(SocketError, si)
        #, ["-s","localhost:8518"])


    def testGetStatus(self):
        s = status(self.logger, [])

        #1) missing required -t option
        expRes = CommandResult(1, 'Error: Task option is required')
        res = s()
        self.assertEquals(expRes, res)

        #2) correct execution
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_AnalysisName')
        s = status(self.logger, ["-t", analysisDir])
        res = s()
        expRes = CommandResult(0, None)
        self.assertEquals( expRes, s)

        #3) wrong -t option
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_XXX')
        self.assertRaises( IOError, s, ["-t", analysisDir])


    def testReport(self):
        """
        Test the functionality of the report command
        """

        rep = report(self.logger)

        # Missing required -t option
        expRes = CommandResult(1, 'Error: Task option is required')
        res = rep([])
        self.assertEquals(expRes, res)

        # Executes
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_AnalysisName')
        res = rep(["-t", analysisDir])
        expRes = CommandResult(0, None)
        self.assertEquals(expRes, res)

        # Wrote correct file
        with open('lumiReport.json', 'r') as reportFile:
            result = json.load(reportFile)
            self.assertEquals(result, CRABRESTModelMock.goodLumisResult)


    def testGetOutput(self):
        """
        Crete a fake source output file and verify it is copied to the correct
        dest directory
        """
        #f = open("src_output.root", 'w')
        #f.close()

        #1) missing required -t option (the other required option, -r, is ignored)
        go = getoutput(self.logger, [])
        res = go()
        expRes = CommandResult(1, 'Error: Task option is required')

        #2) -t option is present but -r is missing
        analysisDir = os.path.join(os.path.dirname(__file__), 'crab_AnalysisName')
        go = getoutput(self.logger, ["-t", analysisDir])
        res = go()
        expRes = CommandResult(1, 'Error: Range option is required')

        #3) request passed with the -t option does not exist
        #res = go(["-t", analysisDir + "asdf"])
        #TODO we expect an appropriate answer from the server

        #4) check correct behaviour without specifying output directory
        #N.B.: -p options is required for tests to skip proxy creation and delegation
        go = getoutput(self.logger, ["-t", analysisDir, "-r", "20", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        destDir = os.path.join(analysisDir, 'results')
        self.assertTrue(os.path.isdir(destDir))
        #Remove the directory
        shutil.rmtree(destDir)
        self.assertFalse(os.path.isdir(destDir))

        #5) correct behavior and output directory specified which exists
        go = getoutput(self.logger, ["-t", analysisDir, "-r", "20", "-o", "/tmp", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        self.assertTrue(os.path.isdir('/tmp'))
        #TODO check tath the file has been copied

        #6) correct behavior and output directory specified which does not exists
        go = getoutput(self.logger, ["-t", analysisDir, "-r", "20", "-o", "/tmp/asdf/qwerty", "-p"])
        res = go()
        expRes = CommandResult(0, None)
        #check if the result directory has been created
        self.assertTrue(os.path.isdir('/tmp/asdf/qwerty'))
        #Remove the directory
        shutil.rmtree('/tmp/asdf/qwerty')



if __name__ == "__main__":
    unittest.main()

