"""
PrivateMC job type plug-in
"""

import os

from CRABClient.ClientUtilities import colors
from CRABClient.JobType.Analysis import Analysis
from CRABClient.ClientMapping import getParamDefaultValue


class PrivateMC(Analysis):
    """
    PrivateMC job type plug-in
    """

    def run(self, *args, **kwargs):
        """
        Override run() for JobType
        """
        tarFilename, configArguments = super(PrivateMC, self).run(*args, **kwargs)
        configArguments['jobtype'] = 'PrivateMC'

        lhe, nfiles = self.cmsswCfg.hasLHESource()
        if lhe:
            self.logger.debug("LHESource found in the CMSSW configuration.")
            configArguments['generator'] = getattr(self.config.JobType, 'generator', 'lhe')

            major, minor, fix = os.environ['CMSSW_VERSION'].split('_')[1:4]
            warn = True
            if int(major) >= 7:
                if int(minor) >= 5:
                    warn = False

            if nfiles > 1 and warn:
                msg = "{0}Warning{1}: Using an LHESource with ".format(colors.RED, colors.NORMAL)
                msg += "more than one input file may not be supported by the CMSSW version used. "
                msg += "Consider merging the LHE input files to guarantee complete processing."
                self.logger.warning(msg)

        configArguments['primarydataset'] = getattr(self.config.Data, 'outputPrimaryDataset', 'CRAB_PrivateMC')

        return tarFilename, configArguments


    def validateConfig(self, config):
        """
        Validate the PrivateMC portion of the config file making sure
        required values are there and optional values don't conflict. Subclass to CMSSW for most of the work
        """
        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return valid, reason

        ## If publication is True, check that there is a primary dataset name specified.
        if getattr(config.Data, 'publication', getParamDefaultValue('Data.publication')):
            if not getattr(config.Data, 'outputPrimaryDataset'):
                msg  = "Invalid CRAB configuration: Parameter Data.outputPrimaryDataset not specified."
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

        ## Make sure the splitting algorithm is valid.
        allowedSplitAlgos = ['EventBased']
        if self.splitAlgo not in allowedSplitAlgos:
            msg  = "Invalid CRAB configuration: Parameter Data.splitting has an invalid value ('%s')." % (self.splitAlgo)
            msg += "\nMC generation job type only supports the following splitting algorithms: %s." % (allowedSplitAlgos)
            return False, msg

        return True, "Valid configuration"
