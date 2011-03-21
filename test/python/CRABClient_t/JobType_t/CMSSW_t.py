#! /usr/bin/env python

"""
_CMSSW_t_

Unittests for CMSSW JobType
"""

import logging
import os
import unittest

from JobType.CMSSW import CMSSW
from JobType.ScramEnvironment import ScramEnvironment

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
        cmssw.run()

    def testOutputFiles(self):
        """
        Make sure return arguments are modified to reflect files
        to be written
        """
        outputFiles = ['histograms.root', 'output.root', 'output2.root']
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run()
        self.assertEqual(configArguments['outputFiles'], outputFiles)
        self.assertTrue(configArguments['userSandbox'])
        self.assertTrue(os.path.getsize(configArguments['userSandbox']) > 0)

    def testScramOut(self):
        """
        Make sure return arguments contain SCRAM info
        """
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run()
        self.assertEqual(configArguments['ScramArch'],    os.environ['SCRAM_ARCH'])
        self.assertEqual(configArguments['CMSSWVersion'], os.environ['CMSSW_VERSION'])

    def testSpecKeys(self):
        """
        Make sure return arguments contain other stuff eventually in WMSpec
        """
        cmssw = CMSSW(config=testWMConfig, logger=self.logger, workingdir=None)
        _dummy, configArguments = cmssw.run()
        self.assertTrue(len(configArguments['InputDataset']) > 0)
        self.assertTrue(configArguments.has_key('ProcessingVersion'))
        self.assertTrue(configArguments.has_key('AnalysisConfigCacheDoc'))


if __name__ == '__main__':
    unittest.main()
