"""
This is a temporary file used to help transitioning to a
CRABClient which has no dependency on WMCore. Its only purpose
is to allow users to keep having this line in crabConfig.py
from WMCore.Configuration import Configuration
instead of :
from CRABClient.Configuration import Configuration

File is meant to be renamed and moved by the CRABClient build procedure  
(crab-build.file in cmsdist or an ad hoc setup script in case a developer wants
to run using source from GH) so that python discovers it as
WMCore.Configuration. When executed, it prints a simple warning
asking the user to update the configuration and calls
CRABClient.Configuration. In CRABClient.Configuration we have
a modified (and frozen) version of WMCore.Configuration which
runs in both python2 and python3 without requiring external
dependencies which are not available in CMSSW_8 or earlies, e.g. "future"
"""

from CRABClient.Configuration import Configuration as Config
from CRABClient.ClientUtilities import colors

import logging
logger = logging.getLogger("CRAB3.all")

class Configuration(Config):
    def __init__(self):
        msg = ''
        msg += '%sWarning: Please update your config file to use configuration from CRABClient instead of WMCore.\n' % (colors.RED)
        msg += 'Change it from "from WMCore.Configuration import Configuration" to "from CRABClient.Configuration import Configuration%s"' % (colors.NORMAL)
        logger.info(msg)
        Config.__init__(self)
        
