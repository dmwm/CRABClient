#! /usr/bin/env python

"""
_CMSSWConfig_t_

Unittests for CMSSW config files
"""

import logging
import os
import unittest

from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from WMCore.Configuration import Configuration

#### Test WMCore.Configuration

testWMConfig = Configuration()

testWMConfig.section_("JobType")
testWMConfig.JobType.pluginName  = 'CMSSW'
testWMConfig.section_("Data")
testWMConfig.Data.inputDataset = '/cms/data/set'
testWMConfig.section_("General")
testWMConfig.General.serverUrl    = 'crabas.lnl.infn.it:8888'
testWMConfig.section_("User")
testWMConfig.User.group    = 'Analysis'

#### Test CMSSW python config

testCMSSWConfig = """
import FWCore.ParameterSet.Config as cms

process = cms.Process("ANALYSIS")
process.load("FWCore.MessageLogger.MessageLogger_cfi")

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(5)
)
process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring('/store/RelVal/2007/7/10/RelVal-RelVal152Z-MM-1184071556/0000/14BFDE39-1B2F-DC11-884F-000E0C3F0521.root')
)
process.TFileService = cms.Service("TFileService",
    fileName = cms.string('histograms.root')
)
process.copyAll = cms.OutputModule("PoolOutputModule",
    fileName = cms.untracked.string('output.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string('Out1'),
    ),
)
process.copySome = cms.OutputModule("PoolOutputModule",
    fileName = cms.untracked.string('output2.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string('Out2'),
    ),
)

process.out = cms.EndPath(process.copyAll+process.copySome)
"""


class CMSSWConfigTest(unittest.TestCase):
    """
    unittest for ScramEnvironment class

    """

    # Set up a dummy logger
    logger = logging.getLogger('UNITTEST')
    logger.setLevel(logging.ERROR)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    logger.addHandler(ch)


    def setUp(self):
        """
        Set up for unit tests
        """

        # Write a test python config file to run tests on
        with open('unittest_cfg.py','w') as cfgFile:
            cfgFile.write(testCMSSWConfig)
        self.reqConfig = {}
        self.reqConfig['RequestorDN']    = "/DC=org/DC=doegrids/OU=People/CN=Eric Vaandering 768123"


    def tearDown(self):
        """
        Clean up the files we've spewed all over
        """
        os.unlink('unittest_cfg.py')
        try:
            os.unlink('unit_test_full.py')
        except OSError:
            pass

        return


    def testScram(self):
        """
        Test Scram environment
        """

        msg = "You must set up a CMSSW environment first"
        scram = ScramEnvironment(logger=self.logger)
        self.assertNotEqual(scram.getCmsswVersion(), None, msg)
        self.assertNotEqual(scram.getScramArch(), None, msg)
        self.assertNotEqual(scram.getCmsswBase(), None, msg)


    def testInit(self):
        """
        Test constructor
        """

        cmsConfig = CMSSWConfig(config=None, userConfig='unittest_cfg.py', logger=self.logger)
        self.assertNotEqual(cmsConfig.fullConfig, None)


    def testWrite(self):
        """
        Test writing out to a file
        """
        cmsConfig = CMSSWConfig(config=None, userConfig='unittest_cfg.py', logger=self.logger)
        cmsConfig.writeFile('unit_test_full.py')
        self.assertTrue(os.path.getsize('unit_test_full.py') > 0)


    def testOutputFiles(self):
        """
        Test output file detection
        """

        cmsConfig = CMSSWConfig(config=None, userConfig='unittest_cfg.py', logger=self.logger)
        self.assertEqual(cmsConfig.outputFiles()[0], ['histograms.root'])
        self.assertEqual(cmsConfig.outputFiles()[1], ['output.root', 'output2.root'])


    def testUpload(self):
        """
        Test uploading of output file to CRABServer
        """
        cmsConfig = CMSSWConfig(config=testWMConfig, userConfig='unittest_cfg.py', logger=self.logger)
        cmsConfig.writeFile('unit_test_full.py')
        result = cmsConfig.upload(self.reqConfig)

        self.assertTrue(result[0]['DocID'])



if __name__ == '__main__':
    unittest.main()
