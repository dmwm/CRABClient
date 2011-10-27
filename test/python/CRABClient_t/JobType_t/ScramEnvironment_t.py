#! /usr/bin/env python

"""
_ScramEnvironment_t_

Unittests for ScramEnvironment module
"""

import logging
import os
import unittest

from CRABClient.JobType.ScramEnvironment import ScramEnvironment

class ScramEnvironmentTest(unittest.TestCase):
    """
    unittest for ScramEnvironment class

    """

    def setUp(self):
        """
        Set up for unit tests
        """

        self.testLogger = logging.getLogger('UNITTEST')
        self.testLogger.setLevel(logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        self.testLogger.addHandler(ch)

        # set relevant variables

        self.arch    = 'slc5_ia32_gcc434'
        self.version = 'CMSSW_3_8_7'
        self.base    = '/tmp/CMSSW_3_8_7'

        os.environ['SCRAM_ARCH']    = self.arch
        os.environ['CMSSW_BASE']    = self.base
        os.environ['CMSSW_VERSION'] = self.version

    def tearDown(self):
        """
        Do nothing
        """
        return

    def testInit(self):
        """
        Test constructor
        """

        scram = ScramEnvironment(logger=self.testLogger)
        scram.getCmsswVersion()

    def testAccessors(self):
        """
        Test various accessors
        """

        scram = ScramEnvironment(logger=self.testLogger)

        self.assertEqual(scram.getCmsswVersion(), self.version)
        self.assertEqual(scram.getScramArch(),    self.arch)
        self.assertEqual(scram.getCmsswBase(),    self.base)



if __name__ == '__main__':
    unittest.main()


