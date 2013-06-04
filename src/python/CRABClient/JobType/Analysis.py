"""
CMSSW job type plug-in
"""

import os
import tempfile

import PandaServerInterface as PandaInterface
from FWCore.PythonUtilities.LumiList import LumiList

from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.LumiMask import getLumiMask
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
                           'tfileoutfiles'             : [],
                           'edmoutfiles'               : [],
                          }

        # Get SCRAM environment
        scram = ScramEnvironment(logger=self.logger)

        configArguments.update({'jobarch'    : scram.scramArch,
                                'jobsw' : scram.cmsswVersion, })

        # Build tarball
        if self.workdir:
            tarFilename   = os.path.join(self.workdir, PandaInterface.wrappedUuidGen()+'default.tgz')
            cfgOutputName = os.path.join(self.workdir, 'CMSSW_cfg.py')
        else:
            _dummy, tarFilename   = tempfile.mkstemp(suffix='.tgz')
            _dummy, cfgOutputName = tempfile.mkstemp(suffix='_cfg.py')

        #configArguments['userisburl'] = 'https://'+ self.config.General.ufccacheUrl + '/crabcache/file?hashkey=' + uploadResults['hashkey']#XXX hardcoded
        #configArguments['userisburl'] = 'INSERTuserisburl'#XXX hardcoded
        if getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset
#        configArguments['ProcessingVersion'] = getattr(self.config.Data, 'processingVersion', None)

        # Create CMSSW config
        self.logger.debug("self.config: %s" % self.config)
        self.logger.debug("self.config.JobType.psetName: %s" % self.config.JobType.psetName)
        cmsswCfg = CMSSWConfig(config=self.config, logger=self.logger,
                               userConfig=self.config.JobType.psetName)

        # Interogate CMSSW config and user config for output file names, for now no use for edmFiles or TFiles here.
        analysisFiles, edmFiles = cmsswCfg.outputFiles()
        self.logger.debug("TFiles %s and EDM Files %s will be collected" % (analysisFiles, edmFiles))
        configArguments['tfileoutfiles'] = analysisFiles
        configArguments['edmoutfiles'] = edmFiles

        outputFiles = getattr(self.config.JobType, 'outputFiles', [])
        self.logger.debug("User files %s will be collected" % outputFiles)
        configArguments['addoutputfiles'].extend(outputFiles)

        # Write out CMSSW config
        cmsswCfg.writeFile(cfgOutputName)

        with UserTarball(name=tarFilename, logger=self.logger, config=self.config) as tb:
            inputFiles = getattr(self.config.JobType, 'inputFiles', [])
            tb.addFiles(userFiles=inputFiles, cfgOutputName=cfgOutputName)
            configArguments['adduserfiles'] = [os.path.basename(f) for f in inputFiles]
            uploadResults = tb.upload()

        self.logger.debug("Result uploading input files: %s " % str(uploadResults))
        configArguments['cachefilename'] = uploadResults[1]
        configArguments['cacheurl'] = uploadResults[0]
        isbchecksum = uploadResults[2]

        # Upload lumi mask if it exists
        lumiMaskName = getattr(self.config.Data, 'lumiMask', None)
        if lumiMaskName:
            self.logger.debug("Attaching lumi mask %s to the request" % lumiMaskName)
            lumiDict = getLumiMask(config=self.config, logger=self.logger)
            configArguments['runs'] = lumiDict.keys()
            #for each run we'll encode the lumis as a string representing a list of integers
            #[[1,2],[5,5]] ==> '1,2,5,5'
            configArguments['lumis'] = [ str(reduce(lambda x,y: x+y, \
                                            lumiDict[run]))[1:-1].replace(' ','') \
                                            for run in configArguments['runs'] ]

        configArguments['jobtype'] = 'Analysis'

        return tarFilename, configArguments, isbchecksum


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
