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
import json
import hashlib
import tarfile
import ast
import json

try:
    from FWCore.PythonUtilities.LumiList import LumiList
except Exception:  # pylint: disable=broad-except
    # if FWCore version is not py3 compatible, use our own
    from CRABClient.LumiList import LumiList

from ServerUtilities import BOOTSTRAP_CFGFILE_DUMP, getProxiedWebDir, NEW_USER_SANDBOX_EXCLUSIONS
from ServerUtilities import SERVICE_INSTANCES

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.UserUtilities import curlGetFileFromURL
from CRABClient.ClientUtilities import colors, LOGGERS, getColumn, getJobTypes, DBSURLS
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
# from CRABClient.JobType._AnalysisNoUpload import _AnalysisNoUpload
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.ClientMapping import getParamDefaultValue
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.ClientUtilities import bootstrapDone, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import ClientException, EnvironmentException, ConfigurationException, CachefileNotFoundException
from CRABClient.Commands.SubCommand import ConfigCommand
from CRABClient.ClientMapping import parametersMapping, getParamDefaultValue
from ServerUtilities import uploadToS3, downloadFromS3


class Recover(BasicJobType):
    """
    CMSSW job type plug-in

    Access the configuration of the task that we are submitting with self.config
    """

    def initCRABRest(self):
        """
        - self.crabserver is the destination, where to submit recovery task. already created for us.
        - self.crabserverCopyOfTask is the source, where the failing task was submitted to
        """
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        serverhost = SERVICE_INSTANCES.get(self.config.JobType.copyCatInstance)
        self.crabserverCopyOfTask = serverFactory(hostname=serverhost['restHost'], localcert=self.proxyfilename,
                               localkey=self.proxyfilename, retry=2, logger=self.logger,
                               verbose=False, userAgent='CRABClient')
        self.crabserverCopyOfTask.setDbInstance(serverhost['dbInstance'])

        # after having an instance of the server where the original task was submitted, 
        # we can now set the General.instance to be the destination server
        self.config.General.instance = self.config.JobType.copyCatInstance

    def getTaskDict(self):
        #getting information about the task
        inputlist = {'subresource':'search', 'workflow': self.config.JobType.copyCatTaskname}

        dictret, _, _ = self.crabserverCopyOfTask.get(api='task', data=inputlist)

        task = {}
        self.logger.debug(dictret)
        task['username'] = getColumn(dictret, 'tm_username')
        task['jobarch'] = getColumn(dictret, 'tm_job_arch')
        task['jobsw'] = getColumn(dictret, 'tm_job_sw')
        task['inputdata'] = getColumn(dictret, 'tm_input_dataset')
        # crabclient send none to server and server confuse
        if not task['inputdata']:
            task.pop('inputdata')

        # it is a list in string format
        task['edmoutfiles'] = ast.literal_eval(getColumn(dictret, 'tm_edm_outfiles'))
        task['tfileoutfiles'] = ast.literal_eval(getColumn(dictret, 'tm_tfile_outfiles'))
        task['addoutputfiles'] = ast.literal_eval(getColumn(dictret, 'tm_outfiles'))
        task['userfiles'] = ast.literal_eval(getColumn(dictret, 'tm_user_files'))

        # use for download original task cache
        task['cachefilename'] = getColumn(dictret, 'tm_user_sandbox')
        task['debugfilename'] = getColumn(dictret, 'tm_debug_files')

        task['primarydataset'] = getColumn(dictret, 'tm_primary_dataset')
        task['jobtype'] = getColumn(dictret, 'tm_job_type')
        tmp = ast.literal_eval(getColumn(dictret, 'tm_split_args'))
        task['runs'] = tmp['runs']
        task['lumis'] = tmp['lumis']
        #import pdb; pdb.set_trace()
        tmp = json.loads(getColumn(dictret, 'tm_user_config'))
        if tmp['inputblocks']:
            task['inputblocks'] = tmp['inputblocks']

        if task['jobtype'] == 'PrivateMC':
            task['generator'] = getColumn(dictret, 'tm_generator')

        # if the original task has publication enabled, then the recovery task
        # publishes in the same DBS dataset
        failingTaskPublishName = getColumn(dictret, 'tm_publish_name')
        # remove -0...0 from publish name, see https://github.com/dmwm/CRABServer/issues/4947
        failingTaskPublishName = failingTaskPublishName.replace("-00000000000000000000000000000000", "")
        task['publishname2'] = failingTaskPublishName
        # task['jobtype'] = getColumn(dictret, 'tm_publish_groupname')

        # this needs to be passed in recover.py, needs the full path
        # task["scriptexe"] = getColumn(self.failingCrabDBInfo, 'tm_scriptexe')

        return task

    def run(self, filecacheurl = None):
        """
        Override run() for JobType Recover.

        - 'addoutputfiles': [] -> removed
        - 'tfileoutfiles': [] -> removed, ignoring for the time being
        - 'edmoutfiles': ['output.root'], 
        
        'jobarch': 'el8_amd64_gcc11', 'jobsw': 'CMSSW_13_0_3', 
        'inputdata': '/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM', 
        'cacheurl': 'https://cmsweb-test2.cern.ch/S3/crabcache_dev', 
        'cachefilename': 'f1fed93419f0d25d8d7dd1b7331cff56f50376ebfe0c6c77daf9bfd6da6daade.tar.gz', 
        'debugfilename': 'e435f28faecdb441796d2696b9b6f955108a0217ad602bca6114638272ab9a82.tar.gz',
        'jobtype': 'Analysis'}
        """

        self.initCRABRest()
        jobInfoDict = self.getTaskDict()

        # reupload sandbox with new hash (from sandbox filename)
        newCachefilename = "{}.tar.gz".format(hashlib.sha256(jobInfoDict['cachefilename'].encode('utf-8')).hexdigest())
        localPathCachefilename = os.path.join(self.config.JobType.copyCatWorkdir, "taskconfig", 'sandbox.tar.gz')
        uploadToS3(crabserver=self.crabserver, objecttype='sandbox', filepath=localPathCachefilename,
                   tarballname=newCachefilename, logger=self.logger)

        newDebugfilename = "{}.tar.gz".format(hashlib.sha256(jobInfoDict['debugfilename'].encode('utf-8')).hexdigest())
        newDebugPath = os.path.join(self.workdir, newDebugfilename)

        copyOfTaskCrabConfig = os.path.join(self.config.JobType.copyCatWorkdir, "debug_sandbox", 'debug', 'crabConfig.py')
        copyOfTaskPSet =       os.path.join(self.config.JobType.copyCatWorkdir, "debug_sandbox", 'debug', 'originalPSet.py')
        self.config.JobType.psetName = copyOfTaskPSet
        debugFilesUploadResult = None
        with UserTarball(name=newDebugPath, logger=self.logger, config=self.config,
                         crabserver=self.crabserver, s3tester=self.s3tester) as dtb:

            dtb.addMonFiles()
            try:
                debugFilesUploadResult = dtb.upload(filecacheurl = filecacheurl)
            except Exception as e:
                msg = ("Problem uploading debug_files.tar.gz.\nError message: %s.\n"
                       "More details can be found in %s" % (e, self.logger.logfile))
                LOGGERS['CRAB3'].exception(msg) #the traceback is only printed into the logfile


        configreq = {'dryrun': 0}
        for param in parametersMapping['on-server']:
            default = parametersMapping['on-server'][param]['default']
            config_params = parametersMapping['on-server'][param]['config']
            for config_param in config_params:
                attrs = config_param.split('.')
                temp = self.config
                for attr in attrs:
                    temp = getattr(temp, attr, None)
                    if temp is None:
                        break
                if temp is not None:
                    configreq[param] = temp
                    break
                elif default is not None:
                    configreq[param] = default
                    temp = default
                else:
                    ## Parameter not strictly required.
                    pass
            ## Translate boolean flags into integers.
            if param in ['savelogsflag', 'publication', 'nonprodsw', 'useparent',\
                           'ignorelocality', 'saveoutput', 'oneEventMode', 'nonvaliddata', 'ignoreglobalblacklist',\
                           'partialdataset', 'requireaccelerator']:
                configreq[param] = 1 if temp else 0
            ## Translate DBS URL aliases into DBS URLs.
            elif param in ['dbsurl', 'publishdbsurl']:
                if param == 'dbsurl':
                    dbstype = 'reader'
                elif param == 'publishdbsurl':
                    dbstype = 'writer'
                allowed_dbsurls = DBSURLS[dbstype].values()
                allowed_dbsurls_aliases = DBSURLS[dbstype].keys()
                if configreq[param] in allowed_dbsurls_aliases:
                    configreq[param] = DBSURLS[dbstype][configreq[param]]
                elif configreq[param].rstrip('/') in allowed_dbsurls:
                    configreq[param] = configreq[param].rstrip('/')
            elif param == 'scriptexe' and 'scriptexe' in configreq:
                configreq[param] = os.path.basename(configreq[param])
            elif param in ['acceleratorparams'] and param in configreq:
                configreq[param] = json.dumps(configreq[param])

        configreq.update(jobInfoDict)

        ## RECOVER - set lumimask from crab report, not from original task - START
        ## copied from Analysis.py
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
            run_ranges_is_valid = re.match(r'^\d+((?!(-\d+-))(\,|\-)\d+)*$', run_ranges)
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
            configreq['runs'] = lumi_list.getRuns()
            ## For each run we encode the lumis as a string representing a list of integers: [[1,2],[5,5]] ==> '1,2,5,5'
            lumi_mask = lumi_list.getCompactList()
            configreq['lumis'] = [str(reduce(lambda x,y: x+y, lumi_mask[run]))[1:-1].replace(' ','') for run in configreq['runs']]
        ## RECOVER - set lumimask from crab report, not from original task - END

        ## RECOVER - set userInputFiles from crab report, not from original task - START
        userInputFiles = getattr(self.config.Data, 'userInputFiles', None)
        if userInputFiles:
            configreq['userfiles'] = userInputFiles
        ## RECOVER - set userInputFiles from crab report, not from original task - START


        # new filename
        configreq['cachefilename'] = newCachefilename
        configreq['debugfilename'] = newDebugfilename
        configreq['debugfilename'] = "%s.tar.gz" % debugFilesUploadResult
        configreq['cacheurl'] = filecacheurl

        # pop
        configreq.pop('username', None)
        configreq.pop('workflow', None)
        configreq.pop('vogroup', None)
        # outputlfndirbase
        configreq.pop('lfn', None)
        configreq.pop('asyncdest', None)

        # optional pop
        if getattr(self.config.Data, 'splitting', None):
            configreq.pop('splitalgo', None)
        if getattr(self.config.Data, 'totalUnits', None):
            configreq.pop('totalunits', None)
        if getattr(self.config.Data, 'unitsPerJob', None):
            configreq.pop('algoargs', None)
        if getattr(self.config.JobType, 'maxJobRuntimeMin', None):
            configreq.pop('maxjobruntime', None)
        if getattr(self.config.Data, 'publication', None) != None:
            configreq.pop('publication', None)
        if getattr(self.config.General, 'transferLogs', None) != None:
            configreq.pop('savelogsflag', None)

        return '', configreq


    def validateConfig(self, config):
        """
        """
        # skip it all for now
        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return valid, reason

        return True, "Valid configuration"

    def validateBasicConfig(self, config):
        """

        """
        return True, "Valid configuration"
