"""
CMSSW job type plug-in
"""

import os
import tempfile

from BasicJobType import BasicJobType
from CMSSWConfig import CMSSWConfig
from UserTarball import UserTarball
from ScramEnvironment import ScramEnvironment

class CMSSW(BasicJobType):
    """
    CMSSW job type plug-in
    """


    def run(self, requestConfig):
        """
        Override run() for JobType
        """
        configArguments = {'outputFiles'            : [],
                           'InputDataset'           : [],
                           'ProcessingVersion'      : '',
                           'AnalysisConfigCacheDoc' : '', }

        # Get SCRAM environment

        scram = ScramEnvironment(logger=self.logger)

        configArguments.update({'ScramArch'    : scram.scramArch,
                                'CMSSWVersion' : scram.cmsswVersion, })

        # Build tarball
        if self.workdir:
            tarFilename   = os.path.join(self.workdir, 'default.tgz')
            cfgOutputName = os.path.join(self.workdir, 'CMSSW_cfg.py')
        else:
            _dummy, tarFilename   = tempfile.mkstemp(suffix='.tgz')
            _dummy, cfgOutputName = tempfile.mkstemp(suffix='_cfg.py')

        with UserTarball(name=tarFilename, logger=self.logger, config=self.config) as tb:
            if getattr(self.config.JobType, 'inputFiles', None) is not None:
                tb.addFiles(userFiles=self.config.JobType.inputFiles)
            uploadResults = tb.upload()

        configArguments['userSandbox'] = uploadResults['url']
        configArguments['userFiles'] = [os.path.basename(f) for f in self.config.JobType.inputFiles]
        configArguments['InputDataset'] = self.config.Data.inputDataset
        configArguments['ProcessingVersion'] = self.config.Data.processingVersion

        # Create CMSSW config
        cmsswCfg = CMSSWConfig(config=self.config, logger=self.logger,
                               userConfig=self.config.JobType.psetName)

        # Interogate CMSSW config for output file names
        for fileList in cmsswCfg.outputFiles():
            self.logger.debug("Adding %s to list of output files" % fileList)
            configArguments['outputFiles'].extend(fileList)

        # Write out CMSSW config
        cmsswCfg.writeFile(cfgOutputName)
        result = cmsswCfg.upload(requestConfig)
        configArguments['AnalysisConfigCacheDoc'] = result[0]['DocID']
        return tarFilename, configArguments


    def validateConfig(self, config):
        """
        Validate the CMSSW portion of the config file making sure
        required values are there and optional values don't conflict
        """

        valid = True
        reason = ''

        if not getattr(config, 'Data', None):
            valid = False
            reason += 'Crab configuration problem: missing Data section. '
        else:
            if not getattr(config.Data, 'processingVersion', None):
                valid = False
                reason += 'Crab configuration problem: missing or null processing version. '
            if not getattr(config.Data, 'inputDataset', None):
                valid = False
                reason += 'Crab configuration problem: missing or null input dataset name. '
        if not getattr(config.JobType, 'psetName', None):
            valid = False
            reason += 'Crab configuration problem: missing or null CMSSW config file name. '

        return (valid, reason)

