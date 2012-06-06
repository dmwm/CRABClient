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

class Analysis(BasicJobType):
    """
    CMSSW job type plug-in
    """


    def run(self, requestConfig):
        """
        Override run() for JobType
        """
        configArguments = {'addoutputfiles'            : [],
                           'adduserfiles'              : [],
#                           'ProcessingVersion'         : '',
                           'configdoc'                 : '',
#                           'ACDCDoc'                   : '',
                          }

        # Get SCRAM environment
        scram = ScramEnvironment(logger=self.logger)

        configArguments.update({'jobarch'    : scram.scramArch,
                                'jobsw' : scram.cmsswVersion, })

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
            configArguments['adduserfiles'] = [os.path.basename(f) for f in inputFiles]
            uploadResults = tb.upload()
        configArguments['userisburl'] = 'https://'+ self.config.General.ufccacheUrl + '/crabcache/file?hashkey=' + uploadResults['hashkey']#XXX hardcoded
        if getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset
#        configArguments['ProcessingVersion'] = getattr(self.config.Data, 'processingVersion', None)

        # Create CMSSW config
        cmsswCfg = CMSSWConfig(config=self.config, logger=self.logger,
                               userConfig=self.config.JobType.psetName)

        # Interogate CMSSW config and user config for output file names, for now no use for edmFiles or TFiles here.
        analysisFiles, edmFiles = cmsswCfg.outputFiles()
        self.logger.debug("WMAgent will collect TFiles %s and EDM Files %s" % (analysisFiles, edmFiles))

        outputFiles = getattr(self.config.JobType, 'outputFiles', [])
        self.logger.debug("WMAgent will collect user files %s" % outputFiles)
        configArguments['addoutputfiles'].extend(outputFiles)

        # Write out CMSSW config
        cmsswCfg.writeFile(cfgOutputName)
        result = cmsswCfg.upload(requestConfig)
        configArguments['configdoc'] = result['DocID']

        # Upload lumi mask if it exists
        lumiMaskName = getattr(self.config.Data, 'lumiMask', None)
        if lumiMaskName:
            self.logger.debug("Uploading lumi mask %s" % lumiMaskName)
            lumiMask = LumiMask(config=self.config, logger=self.logger)
            result = lumiMask.upload(requestConfig)
            self.logger.debug("ACDC Fileset created with DocID %s" % result[0]['Name'])
#            configArguments['ACDCDoc'] = result[0]['Name']

        configArguments['jobtype'] = 'Analysis'

        return tarFilename, configArguments


    def validateConfig(self, config):
        """
        Validate the CMSSW portion of the config file making sure
        required values are there and optional values don't conflict
        """

        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return (valid, reason)

        if not getattr(config.Data, 'inputDataset', None):
            valid = False
            reason += 'Crab configuration problem: missing or null input dataset name. '

        return (valid, reason)

    def validateBasicConfig(self, config):
        """
        Validate the common portion of the config for data and MC making sure
        required values are there and optional values don't conflict
        """

        valid = True
        reason = ''

        if not getattr(config, 'Data', None):
            valid = False
            reason += 'Crab configuration problem: missing Data section. '

        if not getattr(config.JobType, 'psetName', None):
            valid = False
            reason += 'Crab configuration problem: missing or null CMSSW config file name. '

        return (valid, reason)
