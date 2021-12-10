"""
This is a temporary file used to help transitioning to a
CRABClient which has no dependency on WMCore. Its only purpose
is to allow users to keep having this line in crabConfig.py:
from WMCore.Configuration import Configuration
instead of :
from CRABClient.Configuration import Configuration

File is meant to be renamed and moved by the CRABClient build procedure  
(crab-build.file in cmsdist) or an ad hoc setup script in case a developer wants
to run using source from GH, so that python discovers it as
WMCore.Configuration. When executed, the code here prints a simple warning
asking the user to update the configuration and calls
CRABClient.Configuration. In CRABClient.Configuration we have
a modified (and frozen) version of WMCore.Configuration which
runs in both python2 and python3 without requiring external
dependencies which are not available in CMSSW_8 or earlier, e.g. "future"
"""

from __future__ import division
from CRABClient.Configuration import Configuration as Config
from CRABClient.ClientUtilities import colors

import logging
logger = logging.getLogger("CRAB3.all")

class Configuration(Config):
    def __init__(self):
        msg = ''
        msg += '%sWarning: CRABClient does not depend on WMCore anymore.\n' % (colors.RED)
        msg += 'Please update your config file to use configuration from CRABClient instead.\n'
        msg += 'Change inside that file from "from WMCore.Configuration import Configuration"\n'
        msg += 'to "from CRABClient.Configuration import Configuration%s\n"' % (colors.NORMAL)
        msg += 'Support for old style "from WMCore.Configuration import ..." will be remove in future versions of CRAB'
        logger.info(msg)
        Config.__init__(self)
        
