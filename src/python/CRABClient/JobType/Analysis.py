"""
CMSSW job type plug-in
"""

import os
import tempfile
import re

from WMCore.DataStructs.LumiList import LumiList

import PandaServerInterface as PandaInterface

from WMCore.Lexicon import lfnParts
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientExceptions import EnvironmentException, ConfigurationException

class Analysis(BasicJobType):
    """
    CMSSW job type plug-in
    """


    def run(self, filecacheurl = None):
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
            tarUUID =  PandaInterface.wrappedUuidGen()
            self.logger.debug('UNIQUE NAME: tarUUID %s ' % tarUUID)
            if len(tarUUID):
                tarFilename   = os.path.join(self.workdir, tarUUID +'default.tgz')
                cfgOutputName = os.path.join(self.workdir, 'CMSSW_cfg.py')
            else:
                raise EnvironmentException('Problem with uuidgen while preparing for Sandbox upload.')
        else:
            _dummy, tarFilename   = tempfile.mkstemp(suffix='.tgz')
            _dummy, cfgOutputName = tempfile.mkstemp(suffix='_cfg.py')

        if getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset
#        configArguments['ProcessingVersion'] = getattr(self.config.Data, 'processingVersion', None)

        # Create CMSSW config
        self.logger.debug("self.config: %s" % self.config)
        self.logger.debug("self.config.JobType.psetName: %s" % self.config.JobType.psetName)
        cmsswCfg = CMSSWConfig(config=self.config, logger=self.logger,
                               userConfig=self.config.JobType.psetName)

        ## Interogate CMSSW config and user config for output file names. For now no use for EDM files or TFiles here.
        edmfiles, tfiles = cmsswCfg.outputFiles()
        addoutputFiles = [re.sub(r'^file:', '', file) for file in getattr(self.config.JobType, 'outputFiles', []) if re.sub(r'^file:', '', file) not in edmfiles+tfiles]
        self.logger.debug("The following EDM output files will be collected: %s" % edmfiles)
        self.logger.debug("The following TFile output files will be collected: %s" % tfiles)
        self.logger.debug("The following user output files will be collected: %s" % addoutputFiles)
        configArguments['edmoutfiles'] = edmfiles
        configArguments['tfileoutfiles'] = tfiles
        configArguments['addoutputfiles'].extend(addoutputFiles)

        # Write out CMSSW config
        cmsswCfg.writeFile(cfgOutputName)

        ## UserTarball calls ScramEnvironment which can raise EnvironmentException.
        ## Since ScramEnvironment is already called above and the exception is not
        ## handled, we are sure that if we reached this point it will not raise EnvironmentException.
        ## But otherwise we should take this into account.
        with UserTarball(name=tarFilename, logger=self.logger, config=self.config) as tb:
            inputFiles = [re.sub(r'^file:', '', file) for file in getattr(self.config.JobType, 'inputFiles', [])]
            tb.addFiles(userFiles=inputFiles, cfgOutputName=cfgOutputName)
            configArguments['adduserfiles'] = [os.path.basename(f) for f in inputFiles]
            uploadResults = tb.upload(filecacheurl = filecacheurl)

        self.logger.debug("Result uploading input files: %s " % str(uploadResults))
        configArguments['cacheurl'] = filecacheurl
        configArguments['cachefilename'] = uploadResults[0]
        isbchecksum = uploadResults[1]

        # Upload list of user-defined input files to process as the primary input
        userFileName = getattr(self.config.Data, 'userInputFile', None)
        if userFileName:
            self.logger.debug("Attaching a list of user-specified primary input files from %s." % userFileName)
            fnames = []
            for fname in open(userFileName).readlines():
                fnames.append(fname.strip())
            configArguments['userfiles'] = filter(lambda x: x, fnames) #removing whitelines and empty objects

            primDS = getattr(self.config.Data, 'primaryDataset', None)
            if primDS:
                # Normalizes "foo/bar" and "/foo/bar" to "/foo/bar"
                primDS = "/" + os.path.join(*primDS.split("/"))
                if not re.match("/%(primDS)s.*" % lfnParts, primDS):
                    self.logger.warning("Invalid primary dataset name %s for private MC; publishing may fail" % primDS)
                configArguments['inputdata'] = primDS
            else:
                configArguments['inputdata'] = getattr(self.config.Data, 'inputDataset', '/CRAB_UserFiles')

        lumi_mask_name = getattr(self.config.Data, 'lumiMask', None)
        lumi_list = None
        if lumi_mask_name:
            self.logger.debug("Attaching lumi mask %s to the request" % lumi_mask_name)
            lumi_list = getLumiList(lumi_mask_name, logger = self.logger)
        run_ranges = getattr(self.config.Data, 'runRange', None)
        run_ranges_is_valid = run_ranges is not None and isinstance(run_ranges, str) and re.match('^\d+((?!(-\d+-))(\,|\-)\d+)*$', run_ranges)
        if run_ranges_is_valid:
            run_list = getRunList(run_ranges)
            if lumi_list:
                lumi_list.selectRuns(run_list)
            else:
                if len(run_list) > 50000:
                    msg  = "Data.runRange includes %s runs." % str(len(run_list))
                    msg += " When Data.lumiMask is not specified, Data.runRange can not include more than 50000 runs."
                    raise ConfigurationException(msg)
                lumi_list = LumiList(runs = run_list)
        if lumi_list:
            configArguments['runs'] = lumi_list.getRuns()
            ## For each run we encode the lumis as a string representing a list of integers: [[1,2],[5,5]] ==> '1,2,5,5'
            lumi_mask = lumi_list.getCompactList()
            configArguments['lumis'] = [str(reduce(lambda x,y: x+y, lumi_mask[run]))[1:-1].replace(' ','') for run in configArguments['runs']]

        configArguments['jobtype'] = 'Analysis'

        return tarFilename, configArguments, isbchecksum


    def validateConfig(self, config):
        """
        Validate the CMSSW portion of the config file making sure
        required values are there and optional values don't conflict.
        """

        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return valid, reason

        if not getattr(config.Data, 'inputDataset', None) and not getattr(config.Data, 'userInputFile', None):
            msg  = "Invalid CRAB configuration: Analysis job type requires an input dataset (or a set of user input files) to run on."
            msg += "\nTo specify an input dataset (or a set of user input files) use the parameter Data.inputDataset (or Data.userInputFile)."
            return False, msg

        if getattr(config.Data, 'primaryDataset', None):
            msg  = "Invalid CRAB configuration: Analysis job type does not need a primary dataset name to be specified."
            msg += "\nPlease remove the parameter Data.primaryDataset."
            return False, msg

        if self.splitAlgo == 'EventBased':
            msg  = "Invalid CRAB configuration: Analysis job type does not support event-based splitting."
            msg += "\nPlease set Data.splitting = 'FileBased' or 'LumiBased'."
            return False, msg

        return True, "Valid configuration"


    def validateBasicConfig(self, config):
        """
        Validate the common portion of the config for data and MC making sure
        required values are there and optional values don't conflict.
        """

        self.splitAlgo = getattr(config.Data, 'splitting', None)
        if not self.splitAlgo:
            msg = "Invalid CRAB configuration: Parameter Data.splitting not specified."
            return False, msg

        if not getattr(config.JobType, 'psetName', None):
            msg = "Invalid CRAB configuration: Parameter JobType.psetName not specified."
            return False, msg

        return True, "Valid configuration"
