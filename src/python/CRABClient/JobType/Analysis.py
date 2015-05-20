"""
CMSSW job type plug-in
"""

import os
import tempfile
import string
import re

from WMCore.Lexicon import lfnParts
from WMCore.DataStructs.LumiList import LumiList

import PandaServerInterface as PandaInterface

from CRABClient.ClientUtilities import colors
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientExceptions import EnvironmentException, ConfigurationException
from CRABClient.ClientMapping import getParamDefaultValue


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

        ## Create CMSSW config.
        self.logger.debug("self.config: %s" % (self.config))
        self.logger.debug("self.config.JobType.psetName: %s" % (self.config.JobType.psetName))
        ## The loading of a CMSSW pset in the CMSSWConfig constructor is not idempotent
        ## in the sense that a second loading of the same pset may not produce the same
        ## result. Therefore there is a cache in CMSSWConfig to avoid loading any CMSSW
        ## pset twice. However, some "complicated" psets seem to evade the caching.
        ## Thus, to be safe, keep the CMSSWConfig instance in a class variable, so that
        ## it can be reused later if wanted (for example, in PrivateMC when checking if
        ## the pset has an LHE source) instead of having to load the pset again.
        ## As for what does "complicated" psets mean, Daniel Riley said that there are
        ## some psets where one module modifies the configuration from another module.
        self.cmsswCfg = CMSSWConfig(config=self.config, logger=self.logger,
                                    userConfig=self.config.JobType.psetName)

        ## Interrogate the CMSSW pset for output files (only output files produced by
        ## PoolOutputModule or TFileService are identified automatically). Do this
        ## automatic detection even if JobType.disableAutomaticOutputCollection = True,
        ## so that we can still classify the output files in EDM, TFile and additional
        ## output files in the Task DB (and the job ad).
        ## TODO: Do we really need this classification at all? cmscp and PostJob read
        ## the FJR to know if an output file is EDM, TFile or other.
        edmfiles, tfiles = self.cmsswCfg.outputFiles()
        ## If JobType.disableAutomaticOutputCollection = True, ignore the EDM and TFile
        ## output files that are not listed in JobType.outputFiles.
        if getattr(self.config.JobType, 'disableAutomaticOutputCollection', getParamDefaultValue('JobType.disableAutomaticOutputCollection')):
            outputFiles = [re.sub(r'^file:', '', file) for file in getattr(self.config.JobType, 'outputFiles', [])]
            edmfiles = [file for file in edmfiles if file in outputFiles]
            tfiles = [file for file in tfiles if file in outputFiles]
        ## Get the list of additional output files that have to be collected as given
        ## in JobType.outputFiles, but remove duplicates listed already as EDM files or
        ## TFiles.
        addoutputFiles = [re.sub(r'^file:', '', file) for file in getattr(self.config.JobType, 'outputFiles', []) if re.sub(r'^file:', '', file) not in edmfiles+tfiles]
        self.logger.debug("The following EDM output files will be collected: %s" % edmfiles)
        self.logger.debug("The following TFile output files will be collected: %s" % tfiles)
        self.logger.debug("The following user output files will be collected: %s" % addoutputFiles)
        configArguments['edmoutfiles'] = edmfiles
        configArguments['tfileoutfiles'] = tfiles
        configArguments['addoutputfiles'].extend(addoutputFiles)
        ## Give warning message in case no output file was detected in the CMSSW pset
        ## nor was any specified in the CRAB configuration.
        if not configArguments['edmoutfiles'] and not configArguments['tfileoutfiles'] and not configArguments['addoutputfiles']:
            msg = "%sWarning%s:" % (colors.RED, colors.NORMAL)
            if getattr(self.config.JobType, 'disableAutomaticOutputCollection', getParamDefaultValue('JobType.disableAutomaticOutputCollection')):
                msg += " Automatic detection of output files in the CMSSW configuration is disabled from the CRAB configuration"
                msg += " and no output file was explicitly specified in the CRAB configuration."
            else:
                msg += " CRAB could not detect any output file in the CMSSW configuration"
                msg += " nor was any explicitly specified in the CRAB configuration."
            msg += " Hence CRAB will not collect any output file from this task."
            self.logger.warning(msg)

        # Write out CMSSW config
        self.cmsswCfg.writeFile(cfgOutputName)

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
        userFilesList = getattr(self.config.Data, 'userInputFiles', None)
        if userFilesList:
            self.logger.debug("Attaching list of user-specified primary input files.")
            userFilesList = map(string.strip, userFilesList)
            userFilesList = [file for file in userFilesList if file]
            if len(userFilesList) != len(set(userFilesList)):
                msg  = "%sWarning%s:" % (colors.RED, colors.NORMAL)
                msg += " CRAB configuration parameter Data.userInputFiles contains duplicated entries."
                msg += " Duplicated entries will be removed."    
                self.logger.warning(msg)
            configArguments['userfiles'] = set(userFilesList)
            ## Get the user-specified primary dataset name.
            primaryDataset = getattr(self.config.Data, 'primaryDataset', 'CRAB_UserFiles')
            # Normalizes "foo/bar" and "/foo/bar" to "/foo/bar"
            primaryDataset = "/" + os.path.join(*primaryDataset.split("/"))
            if not re.match("/%(primDS)s.*" % (lfnParts), primaryDataset):
                self.logger.warning("Invalid primary dataset name %s; publication may fail." % (primaryDataset))
            configArguments['inputdata'] = primaryDataset

        lumi_mask_name = getattr(self.config.Data, 'lumiMask', None)
        lumi_list = None
        if lumi_mask_name:
            self.logger.debug("Attaching lumi mask %s to the request" % (lumi_mask_name))
            try:
                lumi_list = getLumiList(lumi_mask_name, logger = self.logger)
            except ValueError as ex:
                msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Failed to load lumi mask %s : %s" % (lumi_mask_name, ex)
                raise ConfigurationException(msg)
        run_ranges = getattr(self.config.Data, 'runRange', None)
        if run_ranges:
            run_ranges_is_valid = re.match('^\d+((?!(-\d+-))(\,|\-)\d+)*$', run_ranges)
            if run_ranges_is_valid:
                run_list = getRunList(run_ranges)
                if lumi_list:
                    lumi_list.selectRuns(run_list)
                    if not lumi_list:
                        msg = "Invalid CRAB configuration: The intersection between the lumi mask and the run range is null."
                        raise ConfigurationException(msg)
                else:
                    if len(run_list) > 50000:
                        msg  = "CRAB configuration parameter Data.runRange includes %s runs." % str(len(run_list))
                        msg += " When Data.lumiMask is not specified, Data.runRange can not include more than 50000 runs."
                        raise ConfigurationException(msg)
                    lumi_list = LumiList(runs = run_list)
            else:
                msg = "Invalid CRAB configuration: Parameter Data.runRange should be a comma separated list of integers or (inclusive) ranges. Example: '12345,99900-99910'"
                raise ConfigurationException(msg)
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

        ## Make sure only one of the two parameters Data.inputDataset and Data.userInputFiles
        ## was specified.
        if hasattr(config.Data, 'inputDataset') and hasattr(config.Data, 'userInputFiles'):
            msg  = "Invalid CRAB configuration: Analysis job type accepts either an input dataset or a set of user input files to run on, but not both."
            msg += "\nSuggestion: Specify only one of the two parameters, Data.inputDataset or Data.userInputFiles, but not both."
            return False, msg

        ## Make sure at least one of the two parameters Data.inputDataset and Data.userInputFiles
        ## was specified.
        if not getattr(config.Data, 'inputDataset', None) and not getattr(config.Data, 'userInputFiles', None):
            msg  = "Invalid CRAB configuration: Analysis job type requires an input dataset or a set of user input files to run on."
            msg += "\nSuggestion: To specify an input dataset use the parameter Data.inputDataset."
            msg += " To specify a set of user input files use the parameter Data.userInputFiles."
            return False, msg

        ## When running over an input dataset, we don't accept that the user specifies a
        ## primary dataset name, because the primary dataset name will already be extracted
        ## from the input dataset name.
        if getattr(config.Data, 'inputDataset', None) and getattr(config.Data, 'primaryDataset', None):
            msg  = "Invalid CRAB configuration: Analysis job type with input dataset does not accept a primary dataset name to be specified."
            msg += "\nSuggestion: Remove the parameter Data.primaryDataset."
            return False, msg

        ## When running over user input files, make sure the splitting mode is 'FileBased'.
        if getattr(config.Data, 'userInputFiles', None) and self.splitAlgo != 'FileBased':
            msg  = "Invalid CRAB configuration: Analysis job type with user input files only supports file-based splitting."
            msg += "\nSuggestion: Set Data.splitting = 'FileBased'."
            return False, msg

        ## Make sure splitting mode is not 'EventBased'.
        if self.splitAlgo == 'EventBased':
            msg  = "Invalid CRAB configuration: Analysis job type does not support event-based splitting."
            msg += "\nSuggestion: Set Data.splitting = 'FileBased' or 'LumiBased'."
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
