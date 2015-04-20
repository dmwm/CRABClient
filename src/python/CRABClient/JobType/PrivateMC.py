"""
PrivateMC job type plug-in
"""

import os
import re

from WMCore.Lexicon import lfnParts

from CRABClient.JobType.Analysis import Analysis
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.ClientMapping import getParamDefaultValue
from CRABClient.ClientUtilities import colors


class PrivateMC(Analysis):
    """
    PrivateMC job type plug-in
    """

    def run(self, *args, **kwargs):
        """
        Override run() for JobType
        """
        tarFilename, configArguments, isbchecksum = super(PrivateMC, self).run(*args, **kwargs)
        configArguments['jobtype'] = 'PrivateMC'

        lhe, nfiles = self.cmsswCfg.hasLHESource()
        if lhe:
            configArguments['generator'] = getattr(self.config.JobType, 'generator', 'lhe')
            if nfiles > 1:
                msg = "{0}Warning{1}: Using an LHESource with ".format(colors.RED, colors.NORMAL)
                msg += "more than one input file may not be supported by the CMSSW version used. "
                msg += "Consider merging the LHE input files to guarantee complete processing."
                self.logger.warning(msg)

        ## Get the user-specified primary dataset name.
        primaryDataset = getattr(self.config.Data, 'primaryDataset', 'CRAB_PrivateMC')
        # Normalizes "foo/bar" and "/foo/bar" to "/foo/bar"
        primaryDataset = "/" + os.path.join(*primaryDataset.split("/"))
        if not re.match("/%(primDS)s.*" % (lfnParts), primaryDataset):
            self.logger.warning("Invalid primary dataset name %s; publication may fail." % (primaryDataset))
        configArguments['inputdata'] = primaryDataset
        return tarFilename, configArguments, isbchecksum

    def validateConfig(self, config):
        """
        Validate the PrivateMC portion of the config file making sure
        required values are there and optional values don't conflict. Subclass to CMSSW for most of the work
        """
        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return valid, reason

        ## Check that there is no input dataset specified.
        if getattr(config.Data, 'inputDataset', None):
            msg  = "Invalid CRAB configuration: MC generation job type does not use an input dataset."
            msg += "\nIf you really intend to run over an input dataset, then you have to run an analysis job type (i.e. set JobType.pluginName = 'Analysis')."
            return False, msg

        ## If publication is True, check that there is a primary dataset name specified.
        if getattr(config.Data, 'publication', getParamDefaultValue('Data.publication')):
            if not hasattr(config.Data, 'primaryDataset'):
                msg  = "Invalid CRAB configuration: Parameter Data.primaryDataset not specified."
                msg += "\nMC generation job type requires this parameter for publication."
                return False, msg

        if not hasattr(config.Data, 'totalUnits'):
            msg  = "Invalid CRAB configuration: Parameter Data.totalUnits not specified."
            msg += "\nMC generation job type requires this parameter to know how many events to generate."
            return False, msg
        elif config.Data.totalUnits <= 0:
            msg  = "Invalid CRAB configuration: Parameter Data.totalUnits has an invalid value (%s)." % (config.Data.totalUnits)
            msg += " It must be a natural number."
            return False, msg

        if self.splitAlgo != 'EventBased':
            msg  = "Invalid CRAB configuration: MC generation job type only supports event-based splitting (i.e. Data.splitting = 'EventBased')."
            return False, msg

        return True, "Valid configuration"
