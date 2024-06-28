"""
ScramEnvironment class
"""

import os
import json
import logging
import subprocess

from CRABClient.ClientExceptions import EnvironmentException
from CRABClient.ClientUtilities import BOOTSTRAP_ENVFILE, bootstrapDone, execute_command

class ScramEnvironment(dict):

    """
        _ScramEnvironment_, a class to determine and cache the user's scram environment.

        The class has two modes of work, depending on the existance of the CRAB3_BOOTSTRAP_DIR variable.
            If it is set loads the environemt from a file inside that deirectory
            Otherwise take the necessary information directly from the environment

        Raises:
    """


    def __init__(self, logger=None):
        self.logger = logger if logger else logging

        if bootstrapDone():
            self.logger.debug("Loading required information from the bootstrap environment file")
            try:
                self.initFromFile()
            except EnvironmentException as ee:
                self.logger.info(str(ee))
                self.logger.info("Will try to find the necessary information from the environment")
                self.initFromEnv()
        else:
            self.logger.debug("Loading required information from the environment")
            self.initFromEnv()

        self.logger.debug("Found %s for %s with base %s" % (self.getCmsswVersion(), self.getScramArch(), self.getCmsswBase()))


    def initFromFile(self):
        """ Init the class taking the required information from the boostrap file
        """

        bootFilename = os.path.join(os.environ['CRAB3_BOOTSTRAP_DIR'], BOOTSTRAP_ENVFILE)
        if not os.path.isfile(bootFilename):
            msg = "The CRAB3_BOOTSTRAP_DIR environment variable is set, but I could not find %s" % bootFilename
            raise EnvironmentException(msg)
        else:
            with open(bootFilename) as fd:
                self.update(json.load(fd))


    def initFromEnv(self):
        """ Init the class taking the required information from the environment
        """
        #self.command = 'scram'  # SB I think this line is not needed
        self["SCRAM_ARCH"] = None

        if 'SCRAM_ARCH' in os.environ:
            self["SCRAM_ARCH"] = os.environ["SCRAM_ARCH"]
        else:
            stdout, _, _ = execute_command(command='scram arch')
            self["SCRAM_ARCH"] = stdout

        try:
            self["CMSSW_BASE"]        = os.environ["CMSSW_BASE"]
            self["CMSSW_VERSION"]     = os.environ["CMSSW_VERSION"]
#            Commenting these two out. I don't think they are really needed
#            self.cmsswReleaseBase = os.environ["CMSSW_RELEASE_BASE"]
#            self.localRT          = os.environ["LOCALRT"]
        except KeyError as ke:
            self["CMSSW_BASE"]        = None
            self["CMSSW_VERSION"]     = None
#            self.cmsswReleaseBase = None
#            self.localRT          = None
            msg = "Please make sure you have setup the CMS environment (cmsenv). Cannot find %s in your env" % str(ke)
            msg += "\nPlease refer to https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookCRAB3Tutorial#Setup_the_environment for how to setup the CMS environment."
            raise EnvironmentException(msg)


    def getCmsswBase(self):
        """
        Determine the CMSSW base (user) directory
        """
        return self["CMSSW_BASE"]


    def getCmsswVersion(self):
        """
        Determine the CMSSW version number
        """
        return self["CMSSW_VERSION"]


    def getScramArch(self):
        """
        Determine the scram architecture
        """
        return self["SCRAM_ARCH"]
