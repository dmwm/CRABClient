"""
CMSSW job type plug-in
"""

import os
import tempfile

from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.LumiMask import LumiMask
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.ScramEnvironment import ScramEnvironment

class CMSSW(BasicJobType):
    """
    CMSSW job type plug-in
    """


    def run(self, requestConfig):
        """
        Override run() for JobType
        """
        configArguments = {'outputFiles'            : [],
                           'userFiles'              : [],
                           'InputDataset'           : '',
                           'ProcessingVersion'      : '',
                           'AnalysisConfigCacheDoc' : '',
                           'ACDCDoc'                : '',
                          }

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
            inputFiles = getattr(self.config.JobType, 'inputFiles', [])
            tb.addFiles(userFiles=inputFiles)
            configArguments['userFiles'] = [os.path.basename(f) for f in inputFiles]
            uploadResults = tb.upload()

        configArguments['userSandbox'] = uploadResults['url']
        configArguments['InputDataset'] = self.config.Data.inputDataset
        configArguments['ProcessingVersion'] = getattr(self.config.Data, 'processingVersion', None)

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

        # Upload lumi mask if it exists
        lumiMaskName = getattr(self.config.Data, 'lumiMask', None)

        if lumiMaskName:
            self.logger.debug("Uploading lumi mask %s" % lumiMaskName)
            lumiMask = LumiMask(config=self.config, logger=self.logger)
            result = lumiMask.upload(requestConfig)
            self.logger.debug("ACDC Fileset created with DocID %s" % result[0]['Name'])
            configArguments['ACDCDoc'] = result[0]['Name']

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
            if not getattr(config.Data, 'inputDataset', None):
                valid = False
                reason += 'Crab configuration problem: missing or null input dataset name. '
        if not getattr(config.JobType, 'psetName', None):
            valid = False
            reason += 'Crab configuration problem: missing or null CMSSW config file name. '

        return (valid, reason)

