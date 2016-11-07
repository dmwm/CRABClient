"""
CMSSW job type plug-in
"""

import os
import re
import math
import shutil
import string
import tempfile
from functools import reduce

from httplib import HTTPException

from WMCore.DataStructs.LumiList import LumiList

import PandaServerInterface as PandaInterface

from ServerUtilities import BOOTSTRAP_CFGFILE_DUMP

from CRABClient.ClientUtilities import colors, LOGGERS
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.ClientMapping import getParamDefaultValue
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.JobType.ScramEnvironment import ScramEnvironment
from CRABClient.ClientUtilities import bootstrapDone, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import ClientException, EnvironmentException, ConfigurationException


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

        if getattr(self.config.Data, 'useParent', False) and getattr(self.config.Data, 'secondaryInputDataset', None):
            msg = "Invalid CRAB configuration: Parameters Data.useParent and Data.secondaryInputDataset cannot be used together."
            raise ConfigurationException(msg)

        # Get SCRAM environment
        scram = ScramEnvironment(logger=self.logger)

        configArguments.update({'jobarch': scram.getScramArch(),
                                'jobsw': scram.getCmsswVersion()})

        # Build tarball
        if self.workdir:
            tarUUID =  PandaInterface.wrappedUuidGen()
            self.logger.debug('UNIQUE NAME: tarUUID %s ' % tarUUID)
            if len(tarUUID):
                tarFilename   = os.path.join(self.workdir, tarUUID + 'default.tgz')
                debugTarFilename = os.path.join(self.workdir, 'debugFiles.tgz')
                cfgOutputName = os.path.join(self.workdir, BOOTSTRAP_CFGFILE)
            else:
                raise EnvironmentException('Problem with uuidgen while preparing for Sandbox upload.')
        else:
            _, tarFilename   = tempfile.mkstemp(suffix='.tgz')
            _, cfgOutputName = tempfile.mkstemp(suffix='_cfg.py')

        if getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset

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

        ## If there is a CMSSW pset, do a basic validation of it.
        if not bootstrapDone() and self.config.JobType.psetName:
            valid, msg = self.cmsswCfg.validateConfig()
            if not valid:
                raise ConfigurationException(msg)

        ## We need to put the pickled CMSSW configuration in the right place.
        ## Here, we determine if the bootstrap script already run and prepared everything
        ## for us. In such case we move the file, otherwise we pickle.dump the pset
        if not bootstrapDone():
            # Write out CMSSW config
            self.cmsswCfg.writeFile(cfgOutputName)
        else:
            # Move the pickled and the configuration files created by the bootstrap script
            self.moveCfgFile(cfgOutputName)

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

        ## UserTarball calls ScramEnvironment which can raise EnvironmentException.
        ## Since ScramEnvironment is already called above and the exception is not
        ## handled, we are sure that if we reached this point it will not raise EnvironmentException.
        ## But otherwise we should take this into account.
        with UserTarball(name=tarFilename, logger=self.logger, config=self.config) as tb:
            inputFiles = [re.sub(r'^file:', '', file) for file in getattr(self.config.JobType, 'inputFiles', [])]
            tb.addFiles(userFiles=inputFiles, cfgOutputName=cfgOutputName)
            configArguments['adduserfiles'] = [os.path.basename(f) for f in inputFiles]
            try:
                # convert from unicode to ascii to make it work with CMSSW_5_3_22
                uploadResult = tb.upload(filecacheurl = filecacheurl.encode('ascii', 'ignore'))
            except HTTPException as hte:
                if 'X-Error-Info' in hte.headers:
                    reason = hte.headers['X-Error-Info']
                    reason_re = re.compile(r'\AFile size is ([0-9]*)B\. This is bigger than the maximum allowed size of ([0-9]*)B\.$')
                    re_match = reason_re.match(reason)
                    if re_match:
                        ISBSize = int(re_match.group(1))
                        ISBSizeLimit = int(re_match.group(2))
                        reason  = "%sError%s:" % (colors.RED, colors.NORMAL)
                        reason += " Input sanbox size is ~%sMB. This is bigger than the maximum allowed size of %sMB." % (ISBSize/1024/1024, ISBSizeLimit/1024/1024)
                        ISBContent = sorted(tb.content, reverse=True)
                        biggestFileSize = ISBContent[0][0]
                        ndigits = int(math.ceil(math.log(biggestFileSize+1, 10)))
                        reason += "\nInput sanbox content sorted by size[Bytes]:"
                        for (size, name) in ISBContent:
                            reason += ("\n%" + str(ndigits) + "s\t%s") % (size, name)
                        raise ClientException(reason)
                raise hte
            except Exception as e:
                msg = ("Impossible to calculate the checksum of the sandbox tarball.\nError message: %s.\n"
                       "More details can be found in %s" % (e, self.logger.logfile))
                LOGGERS['CRAB3'].exception(msg) #the traceback is only printed into the logfile
                raise ClientException(msg)

        debugFilesUploadResult = None
        with UserTarball(name=debugTarFilename, logger=self.logger, config=self.config) as dtb:
            dtb.addMonFiles()
            try:
                debugFilesUploadResult = dtb.upload(filecacheurl = filecacheurl)
            except Exception as e:
                msg = ("Problem uploading debug_files.tar.gz.\nError message: %s.\n"
                       "More details can be found in %s" % (e, self.logger.logfile))
                LOGGERS['CRAB3'].exception(msg) #the traceback is only printed into the logfile

        configArguments['cacheurl'] = filecacheurl
        configArguments['cachefilename'] = "%s.tar.gz" % uploadResult
        if debugFilesUploadResult is not None:
            configArguments['debugfilename'] = "%s.tar.gz" % debugFilesUploadResult
        self.logger.debug("Result uploading input files: %(cachefilename)s " % configArguments)

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
            configArguments['primarydataset'] = getattr(self.config.Data, 'outputPrimaryDataset', 'CRAB_UserFiles')

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

        return tarFilename, configArguments



    def checkAutomaticAvail(self, allowedSplitAlgos):
        scram = ScramEnvironment(logger=self.logger)
        major, minor = [int(v) for v in scram.getCmsswVersion().split('_', 3)[1:-1]]
        if major > 7 or (major == 7 and minor >= 2):
            allowedSplitAlgos.append('Automatic')

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
        if getattr(config.Data, 'inputDataset', None) and getattr(config.Data, 'userInputFiles', None):
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
        ## primary dataset, because the primary dataset will already be extracted from
        ## the input dataset.
        if getattr(config.Data, 'inputDataset', None) and getattr(config.Data, 'outputPrimaryDataset', None):
            msg  = "Invalid CRAB configuration: Analysis job type with input dataset does not accept an output primary dataset name to be specified,"
            msg += " because the later will be extracted from the first."
            msg += "\nSuggestion: Remove the parameter Data.outputPrimaryDataset."
            return False, msg

        ## When running over user input files with publication turned on, we want the
        ## user to specify the primary dataset to be used for publication.
        if getattr(config.Data, 'publication', getParamDefaultValue('Data.publication')):
            if not getattr(config.Data, 'inputDataset', None):
                if not getattr(config.Data, 'outputPrimaryDataset', None):
                    msg  = "Invalid CRAB configuration: Parameter Data.outputPrimaryDataset not specified."
                    msg += "\nAnalysis job type without input dataset requires this parameter for publication."
                    return False, msg

        ## When running over user input files, make sure the splitting mode is 'FileBased'.
        if getattr(config.Data, 'userInputFiles', None) and self.splitAlgo != 'FileBased':
            msg  = "Invalid CRAB configuration: Analysis job type with user input files only supports file-based splitting."
            msg += "\nSuggestion: Set Data.splitting = 'FileBased'."
            return False, msg

        ## Make sure the splitting algorithm is valid.
        allowedSplitAlgos = ['FileBased', 'LumiBased', 'EventAwareLumiBased']

        self.checkAutomaticAvail(allowedSplitAlgos)

        if self.splitAlgo not in allowedSplitAlgos:
            msg  = "Invalid CRAB configuration: Parameter Data.splitting has an invalid value ('%s')." % (self.splitAlgo)
            msg += "\nAnalysis job type only supports the following splitting algorithms: %s." % (allowedSplitAlgos)
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


    def moveCfgFile(self, cfgOutputName):
        bootCfgname = os.path.join(os.environ['CRAB3_BOOTSTRAP_DIR'], BOOTSTRAP_CFGFILE)
        bootCfgPklname = os.path.join(os.environ['CRAB3_BOOTSTRAP_DIR'], BOOTSTRAP_CFGFILE_PKL)
        bootCfgDumpname = os.path.join(os.environ['CRAB3_BOOTSTRAP_DIR'], BOOTSTRAP_CFGFILE_DUMP)
        if not os.path.isfile(bootCfgname) or not os.path.isfile(bootCfgPklname):
            msg = "The CRAB3_BOOTSTRAP_DIR environment variable is set, but I could not find %s or %s" % (bootCfgname, bootCfgPklname)
            raise EnvironmentException(msg)
        else:
            try:
                destination = os.path.dirname(cfgOutputName)
                shutil.move(bootCfgname, destination)
                shutil.move(bootCfgPklname, destination)
                if os.path.isfile(bootCfgDumpname):
                    shutil.move(bootCfgDumpname, destination)
            except Exception as ex:
                msg = "Cannot move either %s or %s to %s. Error is: %s" % (bootCfgname, bootCfgPklname, destination, ex)
                raise EnvironmentException(msg)
