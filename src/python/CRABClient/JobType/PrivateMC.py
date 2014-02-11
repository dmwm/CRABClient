"""
PrivateMC job type plug-in
"""

import os
import re

from CRABClient.JobType.Analysis import Analysis
import WMCore.Lexicon

class PrivateMC(Analysis):
    """
    PrivateMC job type plug-in
    """

    def run(self, requestConfig):
        """
        Override run() for JobType
        """
        tarFilename, configArguments, isbchecksum = super(PrivateMC, self).run(requestConfig)
        configArguments['jobtype'] = 'PrivateMC'
        primDS = getattr(self.config.Data, 'primaryDataset', None)
        if primDS:
            # Normalizes "foo/bar" and "/foo/bar" to "/foo/bar"
            primDS = "/" + os.path.join(*primDS.split("/"))
            if not re.match("/%(primDS)s.*" % WMCore.Lexicon.lfnParts, primDS):
                self.logger.warning("Invalid primary dataset name %s for private MC; publishing may fail" % primDS)
            configArguments['inputdata'] = primDS
        elif getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset
        else:
            configArguments['inputdata'] = "/CRAB_PrivateMC"

        return tarFilename, configArguments, isbchecksum

    def validateConfig(self, config):
        """
        Validate the PrivateMC portion of the config file making sure
        required values are there and optional values don't conflict. Subclass to CMSSW for most of the work
        """
        valid, reason = self.validateBasicConfig(config)
        # Put MC requirements here
        if not valid:
            return (valid, reason)

        if not getattr(config.Data, 'totalUnits', None):
            valid = False
            reason += 'Crab configuration problem: missing or null totalUnits. '

        if self.splitAlgo != 'EventBased':  
            valid = False
            reason += 'MC production jobtype supports EventBased splitting only. '

        return (valid, reason)
