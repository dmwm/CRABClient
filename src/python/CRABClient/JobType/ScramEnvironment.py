"""
ScramEnvironment class
"""

import os
import subprocess
from CRABClient.ClientExceptions import EnvironmentException

class ScramEnvironment(object):

    """
        _ScramEnvironment_, a class to determine and cache the user's scram environment.
    """


    def __init__(self, logger=None):
        self.logger = logger

        self.command = 'scram'
        self.scramArch = None

        if os.environ.has_key("SCRAM_ARCH"):
            self.scramArch = os.environ["SCRAM_ARCH"]
        else:
            # subprocess.check_output([self.command, 'arch']).strip() # Python 2.7 and later
            self.scramArch = subprocess.Popen([self.command, 'arch'],
                                              stdout=subprocess.PIPE)\
                             .communicate()[0].strip()

        try:
            self.cmsswBase        = os.environ["CMSSW_BASE"]
            self.cmsswReleaseBase = os.environ["CMSSW_RELEASE_BASE"]
            self.cmsswVersion     = os.environ["CMSSW_VERSION"]
            self.localRT          = os.environ["LOCALRT"]
        except KeyError:
            self.cmsswBase        = None
            self.cmsswReleaseBase = None
            self.cmsswVersion     = None
            self.localRT          = None
            msg = "Please make sure you have setup the CMS enviroment (cmsenv)."
            msg += "\nPlease refer to https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookCRAB3Tutorial#CMS_environment for how to setup the CMS enviroment."
            raise EnvironmentException(msg)

        self.logger.debug("Found %s for %s with base %s" % (self.cmsswVersion, self.scramArch, self.cmsswBase))


    def getCmsswBase(self):
        """
        Determine the CMSSW base (user) directory
        """
        return self.cmsswBase

    def getCmsswVersion(self):
        """
        Determine the CMSSW version number
        """
        return self.cmsswVersion

    def getScramArch(self):
        """
        Determine the scram architecture
        """
        return self.scramArch
