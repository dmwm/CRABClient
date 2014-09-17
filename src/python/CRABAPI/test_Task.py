import os
import os.path
import shutil
import tempfile
import unittest
import CRABAPI.Abstractions
import CRABClient.Emulator
class Task(unittest.TestCase):
    """
        Test that our functionality is correct (stubbing out CRABClient
        completely)
    """
    def setUp(self):
        self.myTask = CRABAPI.Abstractions.Task()

    def test_Task(self):
        self.assertIsInstance(self.myTask, CRABAPI.Abstractions.Task)

    def test_kill(self):
        self.assertRaises(NotImplementedError, self.myTask.kill)

    def test_getJob(self):
        self.assertRaises(NotImplementedError, getattr, self.myTask, 'jobs')

    def test_getNonexistant(self):
        self.assertRaises(AttributeError, getattr, self.myTask, 'doesntExist')

    def test_submit(self):
        class dummyClient:
            def __init__(*args, **kwargs):
                pass
            def __call__(*args, **kwargs):
                return {'uniquerequestname' :"TestingRequestID" }
        self.myTask.submitClass = dummyClient
        self.assertEqual(self.myTask.submit(), "TestingRequestID")

class DeepTask(unittest.TestCase):
    """
        Test that we actually get back what we want from CRABClient. Don't
        require too much from the internals, just inject enough fake
        dependecies to convince the client it's talking to something real
    """
    def setUp(self):
        self.testDir = tempfile.mkdtemp()

    def tearDown(self):
        CRABClient.Emulator.clearEmulators()
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test_Submit(self):
        class dummyRest:
            def __init__(*args, **kwargs):
                pass
            def get(self, url, req):
                if url == '/crabserver/prod/info':
                    if req == {'subresource': 'version'}:
                        return {'result':["unittest"]},200,""
                    if req == {'subresource': 'backendurls'}:
                        return {'result':["unittest.host"]},200,""
                print "%s -> %s" % (url, req)
            def put(self, url, data):
                if url == '/crabserver/prod/workflow':
                    res = {'result':[{"RequestName" : "UnittestRequest"}]}
                    return res, 200, ""
                print "%s -> %s" % (url, data)
            @staticmethod
            def getCACertPath():
                return "/tmp"
        class dummyUFC:
            def __init__(self, req):
                pass
            def upload(self, name):
                return {'hashkey':'unittest-dummy-tarball'}
        CRABClient.Emulator.setEmulator('rest', dummyRest)
        CRABClient.Emulator.setEmulator('ufc', dummyUFC)
        myTask = CRABAPI.Abstractions.Task()
        myTask.config.section_("General")
        myTask.config.General.requestName   = 'test1'
        myTask.config.General.saveLogs = True
        myTask.config.General.workArea = os.path.join(self.testDir, "unit")
        myTask.config.section_("JobType")
        myTask.config.JobType.pluginName  = 'PrivateMC'
        myTask.config.JobType.psetName    = 'test_pset.py'
        myTask.config.section_("Data")
        myTask.config.Data.inputDataset = '/CrabTestSingleMu'
        myTask.config.Data.splitting = 'EventBased'
        myTask.config.Data.unitsPerJob = 100
        myTask.config.Data.totalUnits = 1000
        myTask.config.Data.publication = True
        myTask.config.Data.publishDataName = 'CRABAPI-Unittest'
        myTask.config.section_("Site")
        myTask.config.Site.storageSite = 'T2_US_Nowhere'
        val = myTask.submit()
        print val
        self.assertEqual(val, 'UnittestRequest')


