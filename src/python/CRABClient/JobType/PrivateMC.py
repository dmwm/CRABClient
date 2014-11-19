"""
PrivateMC job type plug-in
"""

import os
import re

from CRABClient.JobType.Analysis import Analysis
from CRABClient.ClientMapping import getParamDefaultValue


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
        if hasattr(self.config.Data, 'primaryDataset'):
            configArguments['inputdata'] = "/" + self.config.Data.primaryDataset
        else:
            configArguments['inputdata'] = "/CRAB_PrivateMC"
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
