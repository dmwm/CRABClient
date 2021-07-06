# this is an experimantal new feature introduced by Marco and never fully tested/used
# will worry about pylint if and when we decide to use it and look at details
#pylint: skip-file
"""
CopyCat job type plug-in
"""

import os
import re
import math
import shutil
import string
import tempfile
from functools import reduce
from ast import literal_eval

from WMCore.DataStructs.LumiList import LumiList

from ServerUtilities import BOOTSTRAP_CFGFILE_DUMP, getProxiedWebDir, NEW_USER_SANDBOX_EXCLUSIONS
from ServerUtilities import SERVICE_INSTANCES

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.UserUtilities import curlGetFileFromURL
from CRABClient.ClientUtilities import colors, LOGGERS, getColumn
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.ClientMapping import getParamDefaultValue
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.ClientUtilities import bootstrapDone, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import ClientException, EnvironmentException, ConfigurationException, CachefileNotFoundException



class CopyCat(BasicJobType):
    """
    CMSSW job type plug-in
    """
    def getTaskDict(self):
        #getting information about the task
        inputlist = {'subresource':'search', 'workflow': self.config.JobType.copyCatTaskname}
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        serverhost = SERVICE_INSTANCES.get(self.config.JobType.copyCatInstance)
        server = serverFactory(serverhost, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, dummyStatus, dummyReason = server.get(api='task', data=inputlist)
        webdir = getProxiedWebDir(self.config.JobType.copyCatTaskname, serverhost, uri, self.proxyfilename, self.logger.debug)
        if not webdir:
            webdir = getColumn(dictresult, 'tm_user_webdir')

        return dictresult, webdir


    def run(self, filecacheurl = None):
        """
        Override run() for JobType
        """

        taskDict, webdir = self.getTaskDict()
        addoutputfiles = literal_eval(getColumn(taskDict, 'tm_outfiles'))
        tfileoutfiles = literal_eval(getColumn(taskDict, 'tm_tfile_outfiles'))
        edmoutfiles = literal_eval(getColumn(taskDict, 'tm_edm_outfiles'))
        jobarch = getColumn(taskDict, 'tm_job_arch')
        jobsw = getColumn(taskDict, 'tm_job_sw')

        sandboxFilename = os.path.join(self.workdir, 'sandbox.tar.gz')
        curlGetFileFromURL(webdir + '/sandbox.tar.gz', sandboxFilename, self.proxyfilename)

        configArguments = {'addoutputfiles' : addoutputfiles,
                           'tfileoutfiles' : tfileoutfiles,
                           'edmoutfiles' : edmoutfiles,
                           'jobarch' : jobarch,
                           'jobsw' : jobsw,
                          }

        # Maybe the user wnat to change the dataset
        if getattr(self.config.Data, 'inputDataset', None):
            configArguments['inputdata'] = self.config.Data.inputDataset

        ufc = CRABClient.Emulator.getEmulator('ufc')({'endpoint' : filecacheurl, "pycurl": True})
        result = ufc.upload(sandboxFilename, excludeList = NEW_USER_SANDBOX_EXCLUSIONS)
        if 'hashkey' not in result:
            self.logger.error("Failed to upload source files: %s" % str(result))
            raise CachefileNotFoundException

        configArguments['cacheurl'] = filecacheurl
        configArguments['cachefilename'] = "%s.tar.gz" % str(result['hashkey'])

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

        return sandboxFilename, configArguments


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

        return True, "Valid configuration"


    def validateBasicConfig(self, config):
        """
        Validate the common portion of the config for data and MC making sure
        required values are there and optional values don't conflict.
        """
        return True, "Valid configuration"
