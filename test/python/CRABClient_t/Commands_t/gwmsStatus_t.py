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

    def testInvalidWorkflow(self):
        dag = DagmanDataWorkflow(config = self.config, requestarea=self.getTempDir())
        self.assertRaises(Exception, dag.status, 'Invalid Workflow', '', None)
    
    # TODO: need to metaprogram this to handle the different scheduler options
    def testNonExistentLocalhost(self):
        dag = DagmanDataWorkflow(config = self.config, requestarea=self.getTempDir())
        self.assertRaises(InvalidParameter, dag.status, 'localhost_nonexistent', '', None)

    def testJustRootJob(self):
        dag = DagmanDataWorkflow(config = self.config, 
                                 requestarea=self.getTempDir())
        jobInjector = TestJobInjector('gwms_testing', self.config,
                                      tempdir = self.tempdir + '/inj')
        jobInjector.makeDBSObject()
        requestName = jobInjector.makeRootObject()[0]['RequestName']
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

    def testJustRootJobKilled(self):
        dag = DagmanDataWorkflow(config = self.config,
                                 requestarea=self.getTempDir())
        jobInjector = TestJobInjector('gwms_killing', self.config,
                                      tempdir = self.tempdir + '/inj')
        requestName = jobInjector.makeRootObject()[0]['RequestName']
        dag.kill(requestName,True,userdn=jobInjector.getDummyArgs()['userdn'])
        result = dag.status(requestName, '', None)
        self.assertEqual(result, {'status': 'Killed',
                                  'jobdefErrors': [],
                                  'jobSetID': requestName,
                                  'jobsPerStatus': {}, 
                                  'taskFailureMsg': '', 
                                  'failedJobdefs': 0, 
                                  'totalJobdefs': 0, 
                                  'jobList': []})
    
    def testJustRootJobKilledThenResubmitted(self):
        dag = DagmanDataWorkflow(config = self.config,
                                 requestarea=self.getTempDir())
        jobInjector = TestJobInjector('gwms_resub', self.config,
                                      tempdir = self.tempdir + '/inj')
        requestName = jobInjector.makeRootObject()[0]['RequestName']
        dag.kill(requestName,True,userdn=jobInjector.getDummyArgs()['userdn'])
        dag.resubmit(requestName, [], [], userdn=jobInjector.getDummyArgs()['userdn'])
        result = dag.status(requestName, '', None)
        self.assertTrue(result['status'] in ('Idle', 'Running', 'InTransition'))
        del result['status']
        self.assertEqual(result, {'jobdefErrors': [],
                                  'jobSetID': requestName,
                                  'jobsPerStatus': {}, 
                                  'taskFailureMsg': '', 
                                  'failedJobdefs': 0, 
                                  'totalJobdefs': 0, 
                                  'jobList': []})
 
        
    def testJustRootJobKilledThenResubmittedThenKilled(self):
        dag = DagmanDataWorkflow(config = self.config,
                                 requestarea=self.getTempDir())
        jobInjector = TestJobInjector('gwms_killing', self.config,
                                      tempdir = self.tempdir + '/inj')
        requestName = jobInjector.makeRootObject()[0]['RequestName']
        dag.kill(requestName,True,userdn=jobInjector.getDummyArgs()['userdn'])
        dag.resubmit(requestName, [], [], userdn=jobInjector.getDummyArgs()['userdn'])
        dag.kill(requestName,True,userdn=jobInjector.getDummyArgs()['userdn'])
        result = dag.status(requestName, '', None)

        self.assertEqual(result, {'status': 'Killed',
                                  'jobdefErrors': [],
                                  'jobSetID': requestName,
                                  'jobsPerStatus': {}, 
                                  'taskFailureMsg': '', 
                                  'failedJobdefs': 0, 
                                  'totalJobdefs': 0, 
                                  'jobList': []})
    

if __name__ == '__main__':
    unittest.main()
