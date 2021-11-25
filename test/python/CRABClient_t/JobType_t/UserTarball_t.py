#! /usr/bin/env python

"""
_ScramEnvironment_t_

Unittests for ScramEnvironment module
"""

import logging
import os
import subprocess
import tarfile
import unittest

from CRABClient.JobType.UserTarball import UserTarball
from WMCore.Configuration import Configuration
from CRABClient.ClientExceptions import InputFileNotFoundException

testWMConfig = Configuration()

testWMConfig.section_("JobType")
testWMConfig.JobType.pluginName  = 'CMSSW'
testWMConfig.section_("Data")
testWMConfig.Data.inputDataset = '/cms/data/set'
testWMConfig.section_("General")
testWMConfig.General.serverUrl    = 'cms-xen39.fnal.gov:7723'
testWMConfig.section_("User")
testWMConfig.User.group    = 'Analysis'

class UserTarballTest(unittest.TestCase):
    """
    unittest for ScramEnvironment class

    """

    # Set up a dummy logger
    logger = logging.getLogger('UNITTEST')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    logger.addHandler(ch)


    def setUp(self):
        """
        Set up for unit tests
        """

        # Set relevant variables

        self.arch    = 'slc5_ia32_gcc434'
        self.version = 'CMSSW_3_8_7'
        self.base    = '/tmp/CMSSW_3_8_7'

        os.environ['SCRAM_ARCH']    = self.arch
        os.environ['CMSSW_BASE']    = self.base
        os.environ['CMSSW_RELEASE_BASE']    = 'DUMMY'
        os.environ['LOCALRT']    = 'DUMMY'
        os.environ['CMSSW_VERSION'] = self.version

        self.tarBalls = []

        # Make a dummy CMSSW environment

        commands = [
            'rm -rf %s' % self.base,
            'mkdir -p %s/lib/%s/' % (self.base, self.arch),
            'touch %s/lib/%s/libSomething.so' % (self.base, self.arch),
            'touch %s/lib/%s/libSomewhere.so' % (self.base, self.arch),
            'mkdir -p %s/src/Module/Submodule/data/' % (self.base),
            'touch %s/src/Module/Submodule/data/datafile.txt' % (self.base),
            'touch %s/src/Module/Submodule/extra_file.txt' % (self.base),
            'touch %s/src/Module/Submodule/extra_file2.txt' % (self.base),
            'touch %s/src/Module/Submodule/additional_file.txt' % (self.base),
        ]

        for command in commands:
            self.logger.debug("Executing command %s" % command)
            subprocess.check_call(command.split(' '))


    def tearDown(self):
        """
        Clean up the files we've spewed all over
        """
        subprocess.check_call(['rm', '-rf', self.base])
        for filename in self.tarBalls:
            self.logger.debug('Deleting tarball %s' % filename)
            os.unlink(filename)

        return


    def testInit(self):
        """
        Test constructor
        """

        tb = UserTarball(name='default.tgz', logger=self.logger)
        self.assertEqual(os.path.basename(tb.name), 'default.tgz')
        self.tarBalls.append(tb.name)


    def testContext(self):
        """
        Test the object out of context (after TarFile is closed)
        """
        with UserTarball(name='default.tgz', logger=self.logger) as tb:
            self.tarBalls.append(tb.name)
        self.assertRaises(IOError, tb.addFiles)
        self.assertEqual(tarfile.GNU_FORMAT, tb.tarfile.format)


    def testAddFiles(self):
        """
        Test the basic tarball, no userfiles
        """
        members = ['lib', 'lib/slc5_ia32_gcc434', 'lib/slc5_ia32_gcc434/libSomewhere.so',
                   'lib/slc5_ia32_gcc434/libSomething.so', 'src/Module/Submodule/data',
                   'src/Module/Submodule/data/datafile.txt', ]
        with UserTarball(name='default.tgz', logger=self.logger) as tb:
            self.tarBalls.append(tb.name)
            tb.addFiles()
            self.assertEqual(sorted(tb.getnames()), sorted(members))


    def testGlob(self):
        """
        Test globbing and extra files
        """
        userFiles = ['%s/src/Module/Submodule/extra_*.txt' % (self.base),
                     '%s/src/Module/Submodule/additional_file.txt' % (self.base)]

        tb = UserTarball(name='default.tgz', logger=self.logger)
        tb.addFiles(userFiles=userFiles)

        members = ['lib', 'lib/slc5_ia32_gcc434', 'lib/slc5_ia32_gcc434/libSomewhere.so',
                   'lib/slc5_ia32_gcc434/libSomething.so', 'src/Module/Submodule/data',
                   'src/Module/Submodule/data/datafile.txt', 'extra_file2.txt',
                   'extra_file.txt', 'additional_file.txt']

        self.assertEqual(sorted(tb.getnames()), sorted(members))
        self.tarBalls.append(tb.name)


    def testMissingGlob(self):
        """
        Test globbing and extra files
        """
        userFiles = ['%s/src/Module/Submodule/extra_*.txt' % (self.base),
                     '%s/src/Module/Submodule/missing_file.txt' % (self.base)]

        tb = UserTarball(name='default.tgz', logger=self.logger)

        self.assertRaises(InputFileNotFoundException, tb.addFiles, userFiles=userFiles)
        self.tarBalls.append(tb.name)


    def testAccess(self):
        """
        Test accesses with __getattr__ to the underlying TarFile.
        This test really should be done with assertRaises as a context manager
        which is only available in python 2.7
        """

        tb = UserTarball(name='default.tgz', logger=self.logger)

        try:
            tb.doesNotExist()
            self.fail('Did not raise AttributeError')
        except AttributeError:
            pass

        try:
            x = tb.doesNotExistEither
            self.fail('Did not raise AttributeError')
        except AttributeError:
            pass


    def testUpload(self):
        """
        Test uploading to a crab server
        """

        tb = UserTarball(name='default.tgz', logger=self.logger, config=testWMConfig)
        result = tb.upload()
        self.assertTrue(result['size'] > 0)
        self.assertTrue(len(result['hashkey']) > 0)


if __name__ == '__main__':
    unittest.main()

