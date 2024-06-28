#! /usr/bin/env python

"""
_CMSSW_t_

Unittests for CMSSW JobType. These tests need to be pointed at a CRAB Server
with functioning UserFileCache (sandbox) to function (see testWMConfig.General.serverUrl) below.
"""

import copy
import logging
import os
import unittest

from CRABClient.JobType.CMSSW import CMSSW
from CRABClient.JobType.ScramEnvironment import ScramEnvironment

# Re-use and extend simple configs from CMSSWConfig_t

from CMSSWConfig_t import testWMConfig, testCMSSWConfig

class CMSSWTest(unittest.TestCase):
    """
    unittest for CMSSW JobType class

    """

    # Set up a dummy logger
    level = logging.ERROR
    logger = logging.getLogger('UNITTEST')
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    logger.addHandler(ch)

    def setUp(self):
        """
        Set up for unit tests
        """
        # Extend simple config
        testWMConfig.JobType.inputFiles = []
        testWMConfig.JobType.psetName    = 'unittest_cfg.py'
        testWMConfig.Data.processingVersion = 'v1'
        testWMConfig.General.serverUrl    = 'cms-xen39.fnal.gov:7723' # Set your server URL here if needed
        self.reqConfig = {}
        self.reqConfig['RequestorDN']    = "/DC=org/DC=doegrids/OU=People/CN=Eric Vaandering 768123"

        # Write a test python config file to run tests on
        with open(testWMConfig.JobType.psetName,'w') as f:
            f.write(testCMSSWConfig)


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

        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        cmssw.run(self.reqConfig)


    def testOutputFiles(self):
        """
        Make sure return arguments are modified to reflect files
        to be written
        """
        outputFiles = ['histograms.root', 'output.root', 'output2.root']
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run(self.reqConfig)
        self.assertEqual(configArguments['outputFiles'], outputFiles)


    def testSandbox(self):
        """
        Make sure userSandbox is set and it creates a sandbox
        """
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        tarFileName, configArguments = cmssw.run(self.reqConfig)
        self.assertTrue(configArguments['userSandbox'])
        self.assertTrue(os.path.getsize(tarFileName) > 0)


    def testNoInputFiles(self):
        """
        Make sure userSandbox is set and it creates a sandbox even if inputFiles are not set
        """
        del testWMConfig.JobType.inputFiles
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        tarFileName, configArguments = cmssw.run(self.reqConfig)
        self.assertTrue(configArguments['userSandbox'])
        self.assertTrue(os.path.getsize(tarFileName) > 0)


    def testScramOut(self):
        """
        Make sure return arguments contain SCRAM info
        """
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run(self.reqConfig)
        self.assertEqual(configArguments['ScramArch'],    os.environ['SCRAM_ARCH'])
        self.assertEqual(configArguments['CMSSWVersion'], os.environ['CMSSW_VERSION'])


    def testSpecKeys(self):
        """
        Make sure return arguments contain other stuff eventually in WMSpec
        """
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run(self.reqConfig)
        self.assertTrue(len(configArguments['InputDataset']) > 0)
        self.assertTrue('ProcessingVersion' in configArguments)
        self.assertTrue('AnalysisConfigCacheDoc' in configArguments)


    def testValidateConfig(self):
        """
        Validate config, done as part of the constructor
        """
        origConfig = copy.deepcopy(testWMConfig)

        # Make sure the original config works
        cmssw = CMSSW(config=origConfig, logger=self.logger, workingdir=None)
        valid, reason = cmssw.validateConfig(config=testWMConfig)
        self.assertTrue(valid)
        self.assertEqual(reason, '')

        # Test a couple of ways of screwing up the processing version
        testConfig = copy.deepcopy(testWMConfig)
        testConfig.Data.processingVersion = ''
        self.assertRaises(Exception, CMSSW, config=testConfig, logger=self.logger, workingdir=None)
        del testConfig.Data.processingVersion
        self.assertRaises(Exception, CMSSW, config=testConfig, logger=self.logger, workingdir=None)

        # Test a bad input dataset
        testConfig = copy.deepcopy(testWMConfig)
        testConfig.Data.inputDataset = ''
        self.assertRaises(Exception, CMSSW, config=testConfig, logger=self.logger, workingdir=None)

        # Test a bad psetName
        testConfig = copy.deepcopy(testWMConfig)
        del testConfig.JobType.psetName
        self.assertRaises(Exception, CMSSW, config=testConfig, logger=self.logger, workingdir=None)

        # Test several errors, make sure the reason message catches them all.
        cmssw = CMSSW(config=origConfig, logger=self.logger, workingdir=None)
        testConfig = copy.deepcopy(testWMConfig)
        testConfig.Data.processingVersion = ''
        testConfig.Data.inputDataset = ''
        del testConfig.JobType.psetName
        valid, reason = cmssw.validateConfig(config=testConfig)
        self.assertFalse(valid)
        self.assertEqual(reason.count('.'), 3)



if __name__ == '__main__':
    unittest.main()
