"""
    gwmsStatus_t.py - Verify condor functionality for the status command
"""

import unittest
import os.path
import shutil
import tempfile
from CRABClient.Commands.status import status as StatusClass
from CRABInterface.DagmanDataWorkflow import DagmanDataWorkflow
from WMCore.Configuration import Configuration
from WMCore.REST.Error import InvalidParameter
from CRABInterface.Dagman.TestJobInjector import TestJobInjector
class TestStatus(unittest.TestCase):
    def setUp(self):
        config = Configuration()
        self.config = config
        self.tempdir = None

    def tearDown(self):
        TestJobInjector.tearDown()
        if self.tempdir and os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)

    def getTempDir(self):
        self.tempdir = tempfile.mkdtemp()
        return self.tempdir

    def testGetOutputFilesFromRoot(self):
        """
        FIXME: flag this test as integration only
        """
        dag = DagmanDataWorkflow(config = self.config, 
                                 requestarea=self.getTempDir())
        jobInjector = TestJobInjector('gwms_testing', self.config,
                                      tempdir = self.tempdir + '/inj')
        requestName = jobInjector.makeRootObject(testReturnStdoutStderr = True )[0]['RequestName']
        result = dag.status(requestName, '', None)
        self.assertTrue( result['status'] in ('Running', 'Idle', 'InTransition') )
        del result['status']
        self.assertEqual(result, {'jobdefErrors': [],
                                  'jobSetID': requestName,
                                  'jobsPerStatus': {}, 
                                  'taskFailureMsg': '', 
                                  'failedJobdefs': 0, 
                                  'totalJobdefs': 0, 
                                  'jobList': []})


if __name__ == '__main__':
    unittest.main()
