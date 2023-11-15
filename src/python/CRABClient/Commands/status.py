from __future__ import division # I want floating points
from __future__ import print_function

import os
import pickle
import sys
import math
import json
import logging
import time
import tempfile
from ast import literal_eval
from datetime import datetime
from functools import cmp_to_key

if sys.version_info >= (3, 0):
    from urllib.parse import quote  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import quote

from CRABClient.ClientUtilities import colors, validateJobids, compareJobids
from CRABClient.ClientUtilities import PKL_R_MODE
from CRABClient.UserUtilities import curlGetFileFromURL, getColumn
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ConfigurationException
from CRABClient.ClientMapping import parametersMapping

from ServerUtilities import (getEpochFromDBTime, TASKDBSTATUSES_TMP,
                             FEEDBACKMAIL, getProxiedWebDir, isEnoughRucioQuota)

PUBLICATION_STATES = {
    'not_published': 'idle',
    'publication_failed': 'failed',
    'published': 'finished',
    'publishing': 'running',
}

class status(SubCommand):
    """ Query the status of your tasks, or detailed information of one or more tasks
        identified by the -d/--dir option.
    """

    shortnames = ['st']

    def __init__(self, logger, cmdargs=None):
        self.jobids = None
        self.indentation = '\t\t'
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):
        # Get all of the columns from the database for a certain task
        taskname = self.cachedinfo['RequestName']
        server = self.crabserver
        crabDBInfo, _, _ = server.get(api='task', data={'subresource':'search', 'workflow':taskname})
        self.logger.debug("Got information from server oracle database: %s", crabDBInfo)

        # Until the task lands on a schedd we'll show the status from the DB
        dbStatus = getColumn(crabDBInfo, 'tm_task_status')

        user = getColumn(crabDBInfo, 'tm_username')
        webdir = getColumn(crabDBInfo, 'tm_user_webdir')
        rootDagId = getColumn(crabDBInfo, 'clusterid') #that's the condor id from the TW
        publicationEnabled = True if getColumn(crabDBInfo, 'tm_publication') == 'T' else False
        maxMemory = int(getColumn(crabDBInfo, 'tm_maxmemory'))
        maxJobRuntime = int(getColumn(crabDBInfo, 'tm_maxjobruntime'))
        numCores = int(getColumn(crabDBInfo, 'tm_numcores'))
        splitting = getColumn(crabDBInfo, 'tm_split_algo')
        outputLfn = getColumn(crabDBInfo, 'tm_output_lfn')
        outputDestinationSite = getColumn(crabDBInfo, 'tm_asyncdest')
        if getColumn(crabDBInfo, 'tm_output_dataset'):
            outputDatasetList = literal_eval(getColumn(crabDBInfo, 'tm_output_dataset'))
        else:
            outputDatasetList = None
        outDataset = outputDatasetList[0] if outputDatasetList else None # we do not support multiple output datasets anymore

        self.initRucioClient(outputLfn)

        #Print information from the database
        self.printTaskInfo(crabDBInfo, user)
        if not rootDagId:
            failureMsg = "The task has not been submitted to the Grid scheduler yet. Not printing job information."
            self.logger.debug(failureMsg)
            return self.makeStatusReturnDict(crabDBInfo, dbStatus, statusFailureMsg=failureMsg)

        self.logger.debug("The CRAB server submitted your task to the Grid scheduler (cluster ID: %s)" % rootDagId)

        if not webdir:
            # Query condor through the server for information about this task
            params = {'subresource':'taskads', 'workflow':taskname}
            res = server.get(api='workflow', data=params)[0]['result'][0]
            # JobStatus 5 = Held
            if res['JobStatus'] == '5' and 'DagmanHoldReason' in res:
                # If we didn't find a webdir in the DB and the DAG is held,
                # the task bootstrapping failed before or during the webdir
                # upload and the reason should be printed.
                failureMsg = "The task failed to bootstrap on the Grid scheduler. Crab resubmit will not work."
                failureMsg += " Please send an e-mail to %s." % (FEEDBACKMAIL)
                failureMsg += "\nHold reason: %s" % (res['DagmanHoldReason'])
                self.logger.info(failureMsg)
                combinedStatus = "FAILED"
            else:
                # if the dag is submitted and the webdir is not there we have to wait that AdjustSites run
                # and upload the webdir location to the server
                self.logger.info("Waiting for the Grid scheduler to bootstrap your task")
                failureMsg = "Schedd has not reported back the webdir (yet)"
                self.logger.debug(failureMsg)
                combinedStatus = "UNKNOWN"
            return self.makeStatusReturnDict(crabDBInfo, combinedStatus, statusFailureMsg=failureMsg)

        self.logger.debug("Webdir is located at %s", webdir)

        proxiedWebDir = getProxiedWebDir(crabserver=self.crabserver, task=taskname, logFunction=self.logger.debug)

        if not proxiedWebDir:
            msg = "Failed to get the proxied webdir from CRABServer. "
            msg += "\nWill fall back to the regular webdir url for file downloads "
            msg += "but will likely fail if the client is located outside CERN."
            self.logger.debug(msg)
            proxiedWebDir = webdir
        self.logger.debug("Proxied webdir is located at %s", proxiedWebDir)

        # Download status_cache file
        fh, local_status_cache_txt = tempfile.mkstemp(dir='/tmp', prefix='crab_status-cache-', suffix='.txt')
        os.close(fh)  # no need for a hanlde, curl will write using file name
        fh, local_status_cache_pkl = tempfile.mkstemp(dir='/tmp', prefix='crab_status-cache-', suffix='.pkl')
        os.close(fh)  # no need for a handle, curl will write using file name
        gotPickle = False
        gotTxt = False
        # first: try pickle version
        url = proxiedWebDir + "/status_cache.pkl"
        self.logger.debug("Retrieving 'status_cache' file from %s", url)
        try:
            httpCode = curlGetFileFromURL(url, local_status_cache_pkl,
                                                 self.proxyfilename, logger=self.logger)
            if httpCode != 200:
                raise Exception("failed to retrieve %s" % url)
            with open(local_status_cache_pkl, PKL_R_MODE) as fp:
                statusCache = pickle.load(fp)
            os.remove(local_status_cache_pkl)
            if 'bootstrapTime' in statusCache :
                statusCacheInfo_PKL = None
                bootstrapMsg_PKL = "Task bootstrapped at %s" % statusCache['bootstrapTime']['date']
                bootstrapTime = statusCache['bootstrapTime']['fromEpoch']
                bootingTime = int(time.time()) - bootstrapTime
                bootstrapMsg_PKL += ". %d seconds ago" % bootingTime
            else:
                bootstrapMsg_PKL = None
                statusCacheInfo_PKL = statusCache['nodes']
            gotPickle = True
        except Exception as e:  # pylint: disable=unused-variable
            self.logger.debug("%s not found or corrupted. Will use old format file only", url)
            bootstrapMsg_PKL = None
            statusCacheInfo_PKL = None
        # if not running in python3, try also old format
        if sys.version_info < (3, 0):
            url = proxiedWebDir + "/status_cache"
            self.logger.debug("Retrieving 'status_cache' file from %s", url)
            try:
                httpCode = curlGetFileFromURL(url, local_status_cache_txt,
                                                     self.proxyfilename, logger=self.logger)
                if httpCode != 200:
                    raise Exception("failed to retrieve %s" % url)
                with open(local_status_cache_txt, 'r') as fp:
                    statusCacheData = fp.read()
                os.remove(local_status_cache_txt)
                # Normally the first two lines of the file contain the checkpoint locations
                # for the job_log / fjr_parse_results files and are used by the status caching script.
                # But if the job has just bootstrapped the first lines of the file are:
                #  line1: a readable message  line2: bootstrap time in seconds from Epoch
                #  Example:
                #   # Task bootstrapped at 2021-03-22 16:23:54 UTC
                #   1616430956
                # and a dummy, empty, status record follows
                if statusCacheData.split('\n')[0].startswith('#'):
                    statusCacheInfo_TXT = None
                    bootstrapTime = statusCacheData.split('\n')[1]
                    bootstrapMsg_TXT = statusCacheData.split('\n')[0].lstrip('#').lstrip()
                    if bootstrapTime:
                        bootingTime = int(time.time()) - literal_eval(bootstrapTime)
                    else:
                        bootingTime = 199
                    bootstrapMsg_TXT += ". %d seconds ago" % bootingTime
                else:
                    bootstrapMsg_TXT = None
                    # Load the job_report summary
                    statusCacheInfo_TXT = literal_eval(statusCacheData.split('\n')[2])
                gotTxt = True
            except Exception as ce:
                self.logger.debug("%s not found or corrupted.", url)
                statusCacheInfo_TXT = None
                bootstrapMsg_TXT = None
            if not gotTxt and not gotPickle:
                self.logger.info("Waiting for the Grid scheduler to report back the status of your task")
                failureMsg = "Cannot retrieve the status_cache file. Maybe the task process has not run yet?"
                failureMsg += " Got:\n%s" % ce
                self.logger.error(failureMsg)
                logging.getLogger("CRAB3").exception(ce)
                combinedStatus = "UNKNOWN"
                return self.makeStatusReturnDict(crabDBInfo, combinedStatus, statusFailureMsg=failureMsg)

        # should find one of these two in the status_cache file !
        statusCacheInfo = None
        bootstrapMsg = None

        if gotPickle and gotTxt:
            # compare
            if bootstrapMsg_PKL != bootstrapMsg_TXT:
                self.logger.error('bootstrapMsg mismatch PKL vs TXT. Please report this to %s', FEEDBACKMAIL)
            if statusCacheInfo_PKL != statusCacheInfo_TXT:
                self.logger.error('statusCacheInfo mismatch PKL vs TXT Please report this to %s', FEEDBACKMAIL)
        # use old one for py2, pickle for py3
        if sys.version_info >= (3, 0):
            statusCacheInfo = statusCacheInfo_PKL
            bootstrapMsg = bootstrapMsg_PKL
        if sys.version_info < (3, 0):
            statusCacheInfo = statusCacheInfo_TXT
            bootstrapMsg = bootstrapMsg_TXT
        # now code is common again
        if bootstrapMsg:
            self.logger.info(bootstrapMsg)
            if bootingTime  <= 300:
                self.logger.info("Status information will be available within a few minutes")
            else:
                msg = "Task bootstrapped on schedd more than %d minutes ago\n" % (bootingTime//60)
                msg += "But no status info is available yet. If this persists report it to %s " % FEEDBACKMAIL
                self.logger.error(msg)
            combinedStatus = "SUBMITTED"
            statusDict = self.makeStatusReturnDict(crabDBInfo, combinedStatus)
            return statusDict
        if not statusCacheInfo:
            self.logger.error('Format error in status_cache. Empty file ?')
        self.logger.debug("Got information from status cache file: %s", statusCacheInfo)

        automaticSplitt = splitting == 'Automatic'

        # If the task is already on the grid, show the dagman status
        combinedStatus = dagStatus = self.printDAGStatus(dbStatus, statusCacheInfo)

        usingRucio = outputLfn.startswith('/store/user/rucio') or outputLfn.startswith('/store/group/rucio')

        if dagStatus != 'COMPLETED' and usingRucio:
            self.printRucioQuotaInfo(user, outputDestinationSite)

        container = None
        if usingRucio:
            containerInfo = {
                'transferContainerName': getColumn(crabDBInfo, 'tm_transfer_container'),
                'transferRuleID': getColumn(crabDBInfo, 'tm_transfer_rule'),
                'publishRuleID': getColumn(crabDBInfo, 'tm_publish_rule'),
            }
            if containerInfo['transferRuleID']:
                container = containerInfo

        shortResult = self.printOverview(statusCacheInfo, automaticSplitt, proxiedWebDir, container)
        pubStatus = self.printPublication(publicationEnabled, shortResult['jobsPerStatus'], shortResult['numProbes'],
                                          shortResult['numUnpublishable'], taskname, user, crabDBInfo, container=container)
        self.printErrors(statusCacheInfo, automaticSplitt)

        if not self.options.long and not self.options.sort:  # already printed for these options
            self.printDetails(statusCacheInfo, automaticSplitt, self.jobids, True, maxMemory, maxJobRuntime, numCores)

        if self.options.summary:
            self.printSummary(statusCacheInfo)
        if self.options.long or self.options.sort:
            # If user correctly passed some jobid CSVs to use in the status --long, self.jobids
            # will be a list of strings already parsed from the input by the validateOptions()
            if self.jobids:
                # Check the format of the jobids option.
                if self.options.jobids:
                    jobidstuple = validateJobids(self.options.jobids, not automaticSplitt)
                    self.jobids = [str(jobid) for (_, jobid) in jobidstuple]
                self.checkUserJobids(statusCacheInfo, self.jobids)
            sortdict = self.printDetails(statusCacheInfo, automaticSplitt, self.jobids, not self.options.long, maxMemory, maxJobRuntime, numCores)
            if self.options.sort:
                self.printSort(sortdict, self.options.sort)
        if self.options.json:
            self.logger.info(json.dumps(statusCacheInfo))

        statusDict = self.makeStatusReturnDict(crabDBInfo, combinedStatus, dagStatus,
                                               '', shortResult, statusCacheInfo,
                                               pubStatus, proxiedWebDir)

        return statusDict

    def makeStatusReturnDict(self, crabDBInfo, combinedStatus, dagStatus='',
                             statusFailureMsg='', shortResult=None,
                             statusCacheInfo=None, pubStatus=None,
                             proxiedWebDir=''):
        """ Create a dictionary which is mostly identical to the dictionary
            that was being returned by the old status (plus a few other keys
            needed by the other client commands). This is to ensure backward
            compatibility after the status2 transition for users relying on
            this dictionary in their scripts.
        """

        if shortResult is None: shortResult = {}
        if statusCacheInfo is None: statusCacheInfo = {}
        if pubStatus is None: pubStatus = {}

        statusDict = {}
        statusDict['status'] = combinedStatus
        statusDict['dbStatus'] = getColumn(crabDBInfo, 'tm_task_status')
        statusDict['dagStatus'] = dagStatus
        statusDict['username'] = getColumn(crabDBInfo, 'tm_username')
        statusDict['taskFailureMsg'] = getColumn(crabDBInfo, 'tm_task_failure')
        statusDict['taskWarningMsg'] = getColumn(crabDBInfo, 'tm_task_warnings')
        statusDict['outdatasets'] = getColumn(crabDBInfo, 'tm_output_dataset')
        statusDict['schedd'] = getColumn(crabDBInfo, 'tm_schedd')
        statusDict['collector'] = getColumn(crabDBInfo, 'tm_collector')
        statusDict['command'] = getColumn(crabDBInfo, 'tm_task_command')
        statusDict['publicationEnabled'] = True if getColumn(crabDBInfo, 'tm_publication') == 'T' else False
        statusDict['userWebDirURL'] = getColumn(crabDBInfo, 'tm_user_webdir')
        statusDict['inputDataset'] = getColumn(crabDBInfo, 'tm_input_dataset')

        dbStartTime = getColumn(crabDBInfo, 'tm_start_time')
        statusDict['submissionTime'] = getEpochFromDBTime(
            datetime.strptime(dbStartTime, '%Y-%m-%d %H:%M:%S.%f'))

        statusDict['statusFailureMsg'] = statusFailureMsg
        statusDict['proxiedWebDir'] = proxiedWebDir
        statusDict['jobsPerStatus'] = shortResult.get('jobsPerStatus', {})
        statusDict['jobList'] = shortResult.get('jobList', {})
        statusDict['publication'] = pubStatus.get('status', {})
        statusDict['publicationFailures'] = pubStatus.get('failure_reasons', {})
        statusDict['jobs'] = statusCacheInfo
        statusDict['commandStatus'] = 'SUCCESS'
        return statusDict

    def _percentageString(self, state, value, total):
        state = PUBLICATION_STATES.get(state, state)
        digit_count = int(math.ceil(math.log(max(value, total)+1, 10)))
        format_str = "%5.1f%% (%s%" + str(digit_count) + "d%s/%" + str(digit_count) + "d)"
        return format_str % ((value*100/total), self._stateColor(state), value, colors.NORMAL, total)


    def _printState(self, state, ljust):
        state = PUBLICATION_STATES.get(state, state)
        return ('{0}{1:<' + str(ljust) + '}{2}').format(self._stateColor(state), state, colors.NORMAL)

    def _stateColor(self, state):
        if state == 'failed':
            return colors.RED
        elif state == 'running':
            return colors.GREEN
        elif state == 'idle' or state == 'unsubmitted':
            return colors.GRAY
        else:
            return colors.NORMAL

    def publicationStatus(self, workflow, user):
        """Gets some information about the state of publication of jobs from the server.
        """
        api = 'workflow'
        server = self.crabserver

        pubInfo, _, _ = server.get(api, data={'subresource':'publicationstatus', 'workflow':workflow,
                                              'username':user})

        # Dictionary received from the server should have a structure like this:
        # {"result": [
        #  {"status": {"published": 2, "publication_failed": 0, "not_published": 15, "publishing": 0}, "failure_reasons": {}}
        # ]}
        # so return only the inner dict.
        return pubInfo['result'][0]

    @staticmethod
    def translateStatus(statusToTr, dbstatus):
        """Translate from DAGMan internal integer status to a string.

        Uses parameter `dbstatus` to clarify if the state is due to the
        user killing the task.
        """
        statusToTr = {1:'SUBMITTED', 2:'SUBMITTED', 3:'SUBMITTED', 4:'SUBMITTED', 5:'COMPLETED', 6:'FAILED'}[statusToTr]
        # Unfortunately DAG code for killed task is 6, just as like for finished DAGs with failed jobs
        # Relabeling the status from 'FAILED' to 'FAILED (KILLED)' if a successful kill command was issued
        if statusToTr == 'FAILED' and dbstatus == 'KILLED':
            statusToTr = 'FAILED (KILLED)'
        return statusToTr

    @classmethod
    def collapseDAGStatus(cls, dagInfo, dbstatus):
        """Collapse the status of one or several DAGs to a single one.

        Take into account that subdags can be submitted to the queue on the
        schedd, but not yet started.
        """
        status_order = ['SUBMITTED', 'FAILED', 'FAILED (KILLED)', 'COMPLETED']

        subDagInfos = dagInfo.get('SubDags', {})
        subDagStatus = dagInfo.get('SubDagStatus', {})
        # Regular splitting, return status of DAG
        if len(subDagInfos) == 0 and len(subDagStatus) == 0:
            return cls.translateStatus(dagInfo['DagStatus'], dbstatus)

        def check_queued(statusOrSUBMITTED):
            # 99 is the status to expect a submitted DAG. If there are less
            # actual DAG status informations than expected DAGs, at least one
            # DAG has to be queued.
            if dbstatus != 'KILLED' and len(subDagInfos) < len([k for k in subDagStatus if subDagStatus[k] == 99]):
                return 'SUBMITTED'
            return statusOrSUBMITTED

        # If the processing DAG is still running, we are 'SUBMITTED',
        # still.
        if len(subDagInfos) > 0:
            state = cls.translateStatus(subDagInfos[0]['DagStatus'], dbstatus)
            if state == 'SUBMITTED':
                return state
        # Tails active: return most active tail status according to
        # `status_order`
        if len(subDagInfos) > 1:
            states = [cls.translateStatus(subDagInfos[k]['DagStatus'], dbstatus) for k in subDagInfos if k > 0]
            for iStatus in status_order:
                if states.count(iStatus) > 0:
                    return check_queued(iStatus)
        # If no tails are active, return the status of the processing DAG.
        if len(subDagInfos) > 0:
            return check_queued(cls.translateStatus(subDagInfos[0]['DagStatus'], dbstatus))
        return check_queued(cls.translateStatus(dagInfo['DagStatus'], dbstatus))

    def printDAGStatus(self, dbStatus, statusCacheInfo):
        # Get dag status from the node_state/job_log summary
        dagInfo = statusCacheInfo['DagStatus']
        dagStatus = self.collapseDAGStatus(dagInfo, dbStatus)

        msg = "Status on the scheduler:\t" + dagStatus
        self.logger.info(msg)
        return dagStatus

    def printTaskInfo(self, crabDBInfo, username):
        """ Print general information like project directory, task name, scheduler, task status (in the database),
            dashboard URL, warnings and failire messages in the database.
        """
        schedd = getColumn(crabDBInfo, 'tm_schedd')
        if not schedd: schedd = 'N/A yet'
        twname = getColumn(crabDBInfo, 'tw_name')
        if not twname: twname = 'N/A yet'
        statusToPr = getColumn(crabDBInfo, 'tm_task_status')
        command = getColumn(crabDBInfo, 'tm_task_command')
        warnings = literal_eval(getColumn(crabDBInfo, 'tm_task_warnings'))
        failure = getColumn(crabDBInfo, 'tm_task_failure')
        dbStartTime = getColumn(crabDBInfo, 'tm_start_time')

        self.logger.info("CRAB project directory:\t\t%s" % (self.requestarea))
        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        self.logger.info("Grid scheduler - Task Worker:\t%s - %s" % (schedd, twname))
        msg = "Status on the CRAB server:\t"
        if 'FAILED' in statusToPr:
            msg += "%s%s%s" % (colors.RED, statusToPr, colors.NORMAL)
        else:
            if statusToPr in TASKDBSTATUSES_TMP:
                msg += "%s on command %s" % (statusToPr, command)
            else:
                msg += "%s" % (statusToPr)
                if statusToPr == 'TAPERECALL':
                    # a placeholder for possibly printint explanatory messages, it used to be a
                    # pointer to PhEDEx requests for that dataset..
                    # msg += "Status of dataset recalling to disk can be checked via DAS"
                    pass
        self.logger.info(msg)

        # Show server and dashboard URL for the task.
        taskname = quote(self.cachedinfo['RequestName'])

        # CRAB Server UI URL for this task is always useful
        #crabServerUIURL has a format like "https://cmsweb.cern.ch/crabserver/ui/task/" + taskname
        crabServerUIURL = "https://" + self.serverurl + "/crabserver/ui/task/" + taskname
        msg = "%sTask URL to use for HELP:\t%s%s" % (colors.GREEN, crabServerUIURL, colors.NORMAL)
        self.logger.info(msg)

        # Dashboard monitoring URL only makes sense if submitted to schedd
        if schedd:
            # define start and end of timer range for dashboard search: from task creation time to now
            # grafana takes time as milliseconds fom epoch
            try:
                taskCreationTime = getEpochFromDBTime(datetime.strptime(dbStartTime, '%Y-%m-%d %H:%M:%S.%f'))
            except Exception:
                taskCreationTime = int(time.time()) - 3*30*24*60*60 # defaults to now - 3 months
            taskCreationTime = taskCreationTime - 3600  # start looking a bit earlier, avoid rounding  at edges
            taskCreationTime = taskCreationTime * 1000 # from sec to msec
            dashboardURL = "https://monit-grafana.cern.ch/d/cmsTMDetail/cms-task-monitoring-task-view?orgId=11&var-user=" + username \
                         + "&var-task=" + taskname + "&from=" + str(taskCreationTime) + "&to=now"
            self.logger.info("Dashboard monitoring URL:\t%s" % (dashboardURL))

        # Print the warning messages (these are the warnings in the Tasks DB,
        # and/or maybe some warning added by the REST Interface to the status result).
        if warnings:
            warningIndent = '\t\t\t'
            for warningMsg in warnings:
                self.logger.warning("%sWarning%s:%s%s" % (colors.RED, colors.NORMAL, warningIndent, warningMsg.replace('\n', '\n\t'+warningIndent)))
        if failure and 'FAILED' in statusToPr:
            msg = "%sFailure message from server%s:" % (colors.RED, colors.NORMAL)
            msg += "\t%s" % (failure.replace('\n', '\n\t\t\t\t'))
            self.logger.error(msg)

    def checkUserJobids(self, statusCacheInfo, userJobids):
        """ Checks that the job information taken from the status_cache file on the schedd
            contains all of the jobids passed by the user.
        """
        wrongJobIds = [uJobid for uJobid in userJobids if uJobid not in statusCacheInfo.keys()]
        if wrongJobIds:
            raise ConfigurationException("The following jobids were not found in the task: %s" % wrongJobIds)

    def printDetails(self, dictresult, automaticSplitt, jobids=None, quiet=False,
                     maxMemory=parametersMapping['on-server']['maxmemory']['default'],
                     maxJobRuntime=parametersMapping['on-server']['maxjobruntime']['default'],
                     numCores=parametersMapping['on-server']['numcores']['default']):
        """ Print detailed information about a task and each job.
        """
        sortdict = {}
        outputMsg = "\nExtended Job Status Table:\n"
        outputMsg += "\n%4s %-12s %-20s %10s %10s %10s %10s %10s %10s %15s" \
                   % ("Job", "State", "Most Recent Site", "Runtime", "Mem (MB)", "CPU %", "Retries", "Restarts", "Waste", "Exit Code")
        mem_cnt = 0
        mem_min = -1
        mem_max = 0
        mem_sum = 0
        run_cnt = 0
        run_min = -1
        run_max = 0
        run_sum = 0
        cpu_min = -1
        cpu_max = 0
        cpu_sum = 0
        wall_sum = 0

        def translateJobStatus(jobid):
            statusToTr = dictresult[jobid]['State']
            if statusToTr == 'cooloff':
                statusToTr = 'toRetry'
            if not automaticSplitt:
                return statusToTr
            if jobid.startswith('0-') and statusToTr in ('finished', 'failed'):
                return 'no output'
            elif automaticSplitt and '-' not in jobid and statusToTr == 'failed':
                return 'rescheduled'
            return statusToTr

        # Chose between the jobids passed by the user or all jobids that are in the task
        jobidsToUse = jobids if jobids else dictresult.keys()
        for jobid in sorted(jobidsToUse, key=cmp_to_key(compareJobids)):
            info = dictresult[str(jobid)]
            state = translateJobStatus(jobid)
            jobForMetrics = False
            if (state not in ['idle', 'running', 'unsubmitted']) and (not jobid.startswith('0-')): # exclude not-run and probe jobs from metric
                jobForMetrics = True
            site = ''
            if info.get('SiteHistory'):
                site = info['SiteHistory'][-1]
            wall = 0
            if info.get('WallDurations'):
                wall = info['WallDurations'][-1]
                if (run_min == -1) or (wall < run_min): run_min = wall
                if jobForMetrics:
                    if wall > run_max: run_max = wall
            if jobForMetrics:
                run_sum += wall
                run_cnt += 1
            wall_str = to_hms(wall)
            waste = 0
            if info.get('WallDurations'):
                for run in info['WallDurations'][:-1]:
                    waste += run
            if jobForMetrics: wall_sum += waste + wall
            waste = to_hms(waste)
            mem = 'Unknown'
            if info.get('ResidentSetSize'):
                mem = info['ResidentSetSize'][-1]/1024
                if (mem_min == -1) or (wall < mem_min): mem_min = mem
                if jobForMetrics:
                    if mem > mem_max: mem_max = mem
                    mem_sum += mem
                    mem_cnt += 1
                mem = '%d' % mem
            cpu = 'Unknown'
            if (state in ['toRetry', 'failed', 'finished']) and not wall:
                cpu = 0
                if (cpu_min == -1) or cpu < cpu_min: cpu_min = cpu
                if cpu > cpu_max: cpu_max = cpu
                cpu = "%.0f" % cpu
            elif wall and ('TotalSysCpuTimeHistory' in info) and ('TotalUserCpuTimeHistory' in info):
                cpu = info['TotalSysCpuTimeHistory'][-1] + info['TotalUserCpuTimeHistory'][-1]
                if jobForMetrics: cpu_sum += cpu / float(numCores)
                cpu = (cpu / float(wall*numCores)) * 100
                if jobForMetrics:
                    if (cpu_min == -1) or cpu < cpu_min: cpu_min = cpu
                    if cpu > cpu_max: cpu_max = cpu
                cpu = "%.0f" % cpu
            ec = 'Unknown'
            if info['State'] in ['finished']:
                ec = '0'
            elif 'Error' in info:
                ec = str(info['Error'][0]) # exit code of this failed job
            sortdict[str(jobid)] = {'state': state, 'site': site, 'runtime': wall_str, 'memory': mem, 'cpu': cpu, \
                                    'retries': info.get('Retries', 0), 'restarts': info.get('Restarts', 0), 'waste': waste, 'exitcode': ec}
            outputMsg += "\n%4s %-12s %-20s %10s %10s %10s %10s %10s %10s %15s" \
                       % (jobid, state, site, wall_str, mem, cpu, info.get('Retries', 0),
                          info.get('Restarts', 0), waste, ' Postprocessing failed' if ec == '90000' else ec)
        if not quiet:
            self.logger.info(outputMsg)

        # Print (to the log file) a table with the HTCondor cluster id for each job.
        msg = "\n%4s %-10s" % ("Job", "Cluster Id")
        for jobid in jobidsToUse:
            info = dictresult[str(jobid)]
            clusterid = str(info.get('JobIds', 'Unknown'))
            msg += "\n%4s %10s" % (jobid, clusterid)
        if not quiet:
            self.logger.debug(msg)

        if mem_cnt or run_cnt:
            # Print a summary with memory/cpu usage.
            hint = "improve the jobs splitting (e.g. config.Data.splitting = 'Automatic') in a new task"
            usage = {'memory':[mem_max, maxMemory, 0.7, parametersMapping['on-server']['maxmemory']['default'], 'MB'],
                     'runtime':[run_max/60, maxJobRuntime, 0.3, 0, 'min']}
            for param, values in usage.items():
                if values[0] and values[1] > values[3]:
                    if values[0] < values[2]*values[1]:
                        self.logger.info("\n%sWarning%s: the max jobs %s is less than %d%% of the task requested value (%d %s), please consider to request a lower value for failed jobs (allowed through crab resubmit) and/or %s." % (colors.RED, colors.NORMAL, param, values[2]*100, values[1], values[4], hint))
            if run_sum:
                cpu_ave = (cpu_sum / run_sum)
                cpu_thr = 0.5
                cpu_thr_multiThread = 0.3
                if cpu_ave < cpu_thr:
                    cpuMsg = "\n%sWarning%s: the average jobs CPU efficiency is less than %d%%, please consider to " % (colors.RED, colors.NORMAL, cpu_thr*100)
                    if numCores > 1 and cpu_ave < cpu_thr_multiThread:
                        cpuMsg += "request a lower number of threads for failed jobs (allowed through crab resubmit) and/or "
                    self.logger.info(cpuMsg+hint)

            summaryMsg = "\nSummary of run jobs:"
            if mem_cnt:
                summaryMsg += "\n * Memory: %dMB min, %dMB max, %.0fMB ave" % (mem_min, mem_max, mem_sum/mem_cnt)
            if run_cnt:
                summaryMsg += "\n * Runtime: %s min, %s max, %s ave" % (to_hms(run_min), to_hms(run_max), to_hms(run_sum/run_cnt))
            if run_sum and cpu_min >= 0:
                summaryMsg += "\n * CPU eff: %.0f%% min, %.0f%% max, %.0f%% ave" % (cpu_min, cpu_max, cpu_ave*100)
            if wall_sum or run_sum:
                waste = wall_sum - run_sum
                summaryMsg += "\n * Waste: %s (%.0f%% of total)" % (to_hms(waste), (waste / float(wall_sum))*100)
            summaryMsg += "\n"
            self.logger.info(summaryMsg)

        return sortdict

    def printOverview(self, statusCacheInfo, automaticSplitt, proxiedWebDir, container):
        """ Give a summary of the job statuses, keeping in mind that:
                - If there is a job with id 0 then this is the probe job for the estimation
                  This is the so called 'Automatic' splitting
                - Then you have normal jobs
                - Jobs that are line 1-1, 1-2 and so on are completing
        """

        # This record is no longer necessary and makes parsing more difficult.
        if 'DagStatus' in statusCacheInfo:
            del statusCacheInfo['DagStatus']

        jobsPerStatus = {}
        jobList = []
        result = {}
        for job, info in statusCacheInfo.items():
            jobStatus = info['State']
            jobsPerStatus.setdefault(jobStatus, 0)
            jobsPerStatus[jobStatus] += 1
            jobList.append([jobStatus, job])
        result['jobsPerStatus'] = jobsPerStatus
        result['jobList'] = jobList

        # Collect information about probe and tail jobs
        # Create dictionaries like {'finished' : 1, 'running' : 3}
        states = {}
        statesPJ = {}
        statesTJ = {}
        failedProcessing = 0
        for jobid, statusDict in statusCacheInfo.items():
            jobStatus = statusDict['State'] if statusDict['State'] != 'cooloff' else 'toRetry'
            if jobid.startswith('0-'):
                statesPJ[jobStatus] = statesPJ.setdefault(jobStatus, 0) + 1
            elif '-' in jobid:
                statesTJ[jobStatus] = statesTJ.setdefault(jobStatus, 0) + 1
            else:
                states[jobStatus] = states.setdefault(jobStatus, 0) + 1
                if jobStatus == 'failed':
                    failedProcessing += 1
        result['numProbes'] = sum(statesPJ.values())
        result['numUnpublishable'] = 0
        if result['numProbes'] > 0:
            result['numUnpublishable'] = failedProcessing

        def terminate(states, jobStatus, target='no output'):
            if jobStatus in states:
                states[target] = states.setdefault(target, 0) + states.pop(jobStatus)

        toPrint = [('Jobs', states)]
        if self.options.long:
            toPrint = [('Probe jobs', statesPJ), ('Jobs', states), ('Tail jobs', statesTJ)]
            if automaticSplitt:
                toPrint[1] = ('Main jobs', states)
            if sum(statesPJ.values()) > 0:
                terminate(statesPJ, 'failed')
                terminate(statesPJ, 'finished')
                terminate(states, 'failed', target='rescheduled as tail jobs')
        elif sum(statesPJ.values()) > 0:
            if statesTJ:
                states.pop('failed', None) # remove failed from main jobs as they have been rescheduled as tail jobs
                for jobStatus in statesTJ:
                    states[jobStatus] = states.setdefault(jobStatus, 0) + statesTJ[jobStatus]
            else:
                terminate(states, 'failed', target='rescheduled as tail jobs')

        # And if the dictionary is not empty, print it
        if automaticSplitt:
            automaticSplittFAQ = 'https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#What_is_the_Automatic_splitting'
            self.logger.info("\nThe job splitting of this task is 'Automatic', please refer to this FAQ for a description of the jobs status summary:\n%s", automaticSplittFAQ)
            if statesPJ and not states:
                self.logger.info("Probe stage log:\t\t%s", proxiedWebDir+"/AutomaticSplitting_Log0.txt")
            msg = "More details of Automatic Splitting process for this task (including possible failures) are in"
            msg += " the Dagman Log files in:\n%s" % proxiedWebDir+"/AutomaticSplitting/"
            self.logger.info(msg)

        for jobtype, currStates in toPrint:
            if currStates:
                total = sum(currStates[st] for st in currStates)
                state_list = sorted(currStates)
                self.logger.info("\n{0:32}{1}{2}{3}".format(jobtype + ' status:', self._printState(state_list[0], 13), self.indentation, self._percentageString(state_list[0], currStates[state_list[0]], total)))
                for jobStatus in state_list[1:]:
                    self.logger.info("\t\t\t\t{0}{1}{2}".format(self._printState(jobStatus, 13), self.indentation, self._percentageString(jobStatus, currStates[jobStatus], total)))
        self.printRucioInfo(container)
        return result

    def printErrors(self, dictresult, automaticSplitt):
        """ Iterate over dictresult['jobs'] if present, which is a dictionary like:
                {'10': {'State': 'running'}, '1': {'State': 'failed', 'Error' : [10,'error message']}, '3': {'State': 'failed', 'Error' : [10,'error message']} ...
            and group the errors per exit code counting how many jobs exited with a certain exit code, and print the summary
        """
        # In general, there are N_{ec}_{em} jobs that failed with exit code ec and
        # error message em.
        # Since there may be different error messages for a given exit code, we use a
        # map containing the exit code as key and as value a list with the different
        # exit messages. For example:
        # ec_errors = { '1040' : ["Failed due to bla", "Failed due to blabla"],
        #              '60302' : ["File X does not exist", "File Y does not exist"]
        #             }
        ec_errors = {}
        # We also want a map containing again the exit code as key, but as value a list
        # with the lists of job ids that failed with given exit code and error message.
        # For example:
        # ec_jobids = { '1040' : [[1, 2, 3, 4, 5], [10, 12, 14]],
        #              '60302' : [[6, 7, 8, 9, 10], [11, 13, 15]]
        #             }
        # where jobs [1, 2, 3, 4, 5] failed with error message "Failed due to bla",
        # jobs [10, 12, 14] with "Failed due to blabla", jobs [6, 7, 8, 9, 10] with
        # "File X does not exist" and jobs [11, 13, 15] with "File Y does not exist".
        ec_jobids = {}
        # We also want a map containing again the exit code as key, but as value a list
        # with the numbers N_{ec}_{em}. For example:
        # ec_numjobs = { '1040' : [N_{1040}_{"Failed due to bla"}, N_{1040}_{"Failed due to blabla"}],
        #               '60302' : [N_{60302}_{"File X does not exist"}, N_{60302}_{"File Y does not exist"}]
        #              }
        # Actually, for later convenience when sorting by most frequent error messages
        # for each exit code (i.e. sorting by N_{ec}_{em}), we add a second number
        # representing the position of the error message em in the corresponding list
        # in ec_errors[ec]. For example:
        # ec_numjobs = { '1040' : [(N_{1040}_{"Failed due to bla"}, 1), (N_{1040}_{"Failed due to blabla"}, 2)],
        #               '60302' : [(N_{60302}_{"File X does not exist"}, 1), (N_{60302}_{"File Y does not exist"}, 2)]
        #              }
        # We will later sort ec_numjobs[ec], which sorts according to the first number
        # in the 2-tuples. So the sorted ec_numjobs dictionary could be for example:
        # ec_numjobs = { '1040' : [(N_{1040}_{"Failed due to bla"}, 1), (N_{1040}_{"Failed due to blabla"}, 2)],
        #               '60302' : [(N_{60302}_{"File Y does not exist"}, 2), (N_{60302}_{"File X does not exist"}, 1)]
        #              }
        # The second number in the 2-tuples allows to get the error message from
        # ec_errors[ec]: given the sorted ec_numjobs dictionary, for each exit code
        # (i.e. for each key) ec in the dictionary, the most frequent error messages
        # are ec_errors[ec][ec_numjobs[ec][0][1]], ec_errors[ec][ec_numjobs[ec][1][1]], etc.
        ec_numjobs = {}
        unknown = 0
        are_failed_jobs = False

        def consider(jobid):
            if self.options.long:
                return True
            if automaticSplitt and (jobid.startswith('0-') or '-' not in jobid):
                return False
            return True
        for jobid, jobStatus in dictresult.items():
            if jobStatus['State'] == 'failed' and consider(jobid):
                are_failed_jobs = True
                if 'Error' in jobStatus:
                    ec = jobStatus['Error'][0] #exit code of this failed job
                    em = jobStatus['Error'][1] #exit message of this failed job
                    if ec not in ec_errors:
                        ec_errors[ec] = []
                    if ec not in ec_jobids:
                        ec_jobids[ec] = []
                    if ec not in ec_numjobs:
                        ec_numjobs[ec] = []
                    if em not in ec_errors[ec]:
                        ec_numjobs[ec].append((1, len(ec_errors[ec])))
                        ec_errors[ec].append(em)
                        ec_jobids[ec].append([jobid])
                    else:
                        i = ec_errors[ec].index(em)
                        ec_jobids[ec][i].append(jobid)
                        ec_numjobs[ec][i] = (ec_numjobs[ec][i][0] + 1, ec_numjobs[ec][i][1])
                else:
                    unknown += 1
        if are_failed_jobs:
            # If option --sort=exitcodes was specified, show the error summary with the
            # exit codes sorted. Otherwise show it sorted from most frequent exit code to
            # less frequent.
            exitCodes = []
            if self.options.sort == "exitcode":
                for ec in ec_numjobs.keys():
                    if ec not in exitCodes:
                        exitCodes.append(ec)
                exitCodes.sort()
            else:
                numjobsec = []
                for ec, numjobs in ec_numjobs.items():
                    count = 0
                    for nj, _ in numjobs:
                        count += nj
                    numjobsec.append((count, ec))
                numjobsec.sort()
                numjobsec.reverse()
                for _, ec in numjobsec:
                    if ec not in exitCodes:
                        exitCodes.append(ec)
            # Sort the job ids in ec_jobids. Remember that ec_jobids[ec] is a list of lists
            # of job ids, and that each job id is a string.
            for ec in ec_jobids.keys():
                for i in range(len(ec_jobids[ec])):
                    ec_jobids[ec][i] = [str(y) for y in sorted([x for x in ec_jobids[ec][i]], key=cmp_to_key(compareJobids))]
            # Error summary header.
            msg = "\nError Summary:"
            if not self.options.verboseErrors:
                msg += " (use crab status --verboseErrors for details about the errors)"
            # Auxiliary variable for the layout of the error summary messages.
            totnumjobs = len(dictresult)
            ndigits = int(math.ceil(math.log(totnumjobs+1, 10)))
            # For each exit code:
            for i, ec in enumerate(exitCodes):
                # Sort the error messages for this exit code from most frequent one to less
                # frequent one.
                numjobs = sorted(ec_numjobs[ec])
                numjobs.reverse()
                # Count the total number of failed jobs with this exit code.
                count = 0
                for nj, _ in numjobs:
                    count += nj
                # Exit code 90000 means failure in postprocessing stage.
                if ec == 90000:
                    msg += ("\n\n%" + str(ndigits) + "s jobs failed in postprocessing step%s") \
                         % (count, ":" if self.options.verboseErrors else "")
                else:
                    msg += ("\n\n%" + str(ndigits) + "s jobs failed with exit code %s%s") \
                         % (count, ec, ":" if self.options.verboseErrors else "")
                if self.options.verboseErrors:
                    # Costumize the message depending on whether there is only one error message or
                    # more than one.
                    if len(ec_errors[ec]) == 1:
                        error_msg = ec_errors[ec][0]
                        msg += ("\n\n\t%" + str(ndigits) + "s jobs failed with following error message:") % (nj)
                        msg += " (for example, job %s)" % (ec_jobids[ec][0][0])
                        msg += "\n\n\t\t" + "\n\t\t".join([line for line in error_msg.split('\n') if line])
                    else:
                        # Show up to three different error messages.
                        remainder = count
                        if len(ec_errors[ec]) > 3:
                            msg += "\n\t(Showing only the 3 most frequent errors messages for this exit code)"
                        for nj, ne in numjobs[:3]:
                            error_msg = ec_errors[ec][ne]
                            msg += ("\n\n\t%" + str(ndigits) + "s jobs failed with following error message:") % (nj)
                            msg += " (for example, job %s)" % (ec_jobids[ec][ne][0])
                            msg += "\n\n\t\t" + "\n\t\t".join([line for line in error_msg.split('\n') if line])
                            remainder -= nj
                        if remainder > 0:
                            msg += "\n\n\tFor the error messages of the other %s jobs," % (remainder)
                            msg += " please have a look at the dashboard task monitoring web page."
            if unknown:
                msg += "\n\nCould not find exit code details for %s jobs." % (unknown)
            msg += "\n\nHave a look at https://twiki.cern.ch/twiki/bin/viewauth/CMSPublic/JobExitCodes for a description of the exit codes."
            self.logger.info(msg)

    def printSummary(self, dictresult):
        """ Print the information about jobs on each site:
                - How many jobs are or were running on the site and in which state,
                - Runtime for each site.
        """

        sites = {}
        default_info = {"Runtime": 0, "Waste": 0, "Running": 0, "Success": 0, "Failed": 0, "Stageout": 0}
        siteHistory_retrieved = False
        for job in dictresult:
            info = dictresult[job]
            state = info['State']
            site_history = info.get("SiteHistory")
            if not site_history:
                continue
            siteHistory_retrieved = True
            walls = info['WallDurations']
            cur_site = info['SiteHistory'][-1]
            cur_info = sites.setdefault(cur_site, dict(default_info))
            for site, wall in zip(site_history[:-1], walls[:-1]):
                info = sites.setdefault(site, dict(default_info))
                info['Failed'] += 1
                info['Waste'] += wall
            if state in ['failed', 'cooloff', 'held', 'killed'] or (state == 'idle' and cur_site != 'Unknown'):
                cur_info['Failed'] += 1
                cur_info['Waste'] += walls[-1]
            elif state == 'transferring':
                cur_info['Stageout'] += 1
                cur_info['Runtime'] += walls[-1]
            elif state == 'running':
                cur_info['Running'] += 1
                cur_info['Runtime'] += walls[-1]
            elif state == 'finished':
                cur_info['Success'] += 1
                cur_info['Runtime'] += walls[-1]

        # avoid printing header only
        if not siteHistory_retrieved:
            self.logger.info("No SiteHistory retrieved for any job\n")
            return
        if all(site == "Unknown" for site in sites):
            self.logger.info("Site Unknown for any job\n")
            return
        self.logger.info("Site Summary Table (including retries):\n")
        self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % ("Site", "Runtime", "Waste", "Running", "Successful", "Stageout", "Failed"))

        sorted_sites = sorted(sites.keys())
        for site in sorted_sites:
            info = sites[site]
            if site == 'Unknown': continue
            self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % (site, to_hms(info['Runtime']), to_hms(info['Waste']), str(info['Running']), str(info['Success']), str(info['Stageout']), str(info['Failed'])))

        self.logger.info("")

    def printSort(self, sortdict, sortby):
        """ Print information about jobs sorted by a certain attribute.
        """
        sortmatrix = []
        valuedict = {}
        self.logger.info('')
        for jobid in sortdict:
            if sortby in ['exitcode']:
                if sortdict[jobid][sortby] != 'Unknown':
                    value = int(sortdict[jobid][sortby])
                else:
                    value = 999999
                if value not in sortmatrix:
                    sortmatrix.append(value)
                sortmatrix.sort()
                if value not in valuedict:
                    valuedict[value] = [jobid]
                else:
                    valuedict[value].append(jobid)
            elif sortby in ['state', 'site']:
                value = sortdict[jobid][sortby]
                if value not in valuedict:
                    valuedict[value] = [jobid]
                else:
                    valuedict[value].append(jobid)
            elif sortby in ['memory', 'cpu', 'retries']:
                if sortdict[jobid][sortby] != 'Unknown':
                    value = int(sortdict[jobid][sortby])
                else:
                    value = 999999
                sortmatrix.append((value, jobid))
                sortmatrix.sort()
            elif sortby in ['runtime', 'waste']:
                value = sortdict[jobid][sortby]
                realvaluematrix = value.split(':')
                realvalue = 3600*int(realvaluematrix[0]) + 60*int(realvaluematrix[1]) + int(realvaluematrix[2])
                sortmatrix.append((realvalue, value, jobid))
                sortmatrix.sort()
        if sortby in ['exitcode']:
            msg = "Jobs sorted by exit code:\n"
            msg += "\n%-20s %-20s\n" % ('Exit Code', 'Job Id(s)')
            for value in sortmatrix:
                if value == 999999:
                    esignvalue = 'Unknown'
                else:
                    esignvalue = str(value)
                jobids = sorted(valuedict[value], key=cmp_to_key(compareJobids))
                msg += "\n%-20s %-s" % (esignvalue, ", ".join(jobids))
            self.logger.info(msg)
        elif sortby in ['state', 'site']:
            msg = "Jobs sorted by %s:\n" % (sortby)
            msg += "\n%-20s %-20s\n" % (sortby.title(), 'Job Id(s)')
            for value in valuedict:
                msg += "\n%-20s %-s" % (value, ", ".join(valuedict[value]))
            self.logger.info(msg)
        elif sortby in ['memory', 'cpu', 'retries']:
            msg = "Jobs sorted by %s used:\n" % (sortby)
            if sortby == 'memory':
                msg += "%-10s %-10s\n" % ("Memory (MB)".center(10), "Job Id".center(10))
            elif sortby == 'cpu':
                msg += "%-10s %-10s\n" % ("CPU".center(10), "Job Id".center(10))
            elif sortby == 'retries':
                msg += "%-10s %-10s\n" % ("Retries".center(10), "Job Id".center(10))
            for value in sortmatrix:
                if value[0] == 999999:
                    esignvalue = 'Unknown'
                else:
                    esignvalue = value[0]
                msg += "%10s %10s\n" % (str(esignvalue).center(10), value[1].center(10))
            self.logger.info(msg)
        elif sortby in ['runtime', 'waste']:
            msg = "Jobs sorted by %s used:\n" % (sortby)
            msg += "%-10s %-5s\n" % (sortby.title(), "Job Id")
            for value in sortmatrix:
                msg += "%-10s %-5s\n" % (value[1], value[2].center(5))
            self.logger.info(msg)

        self.logger.info('')

    def printRucioInfo(self, container):
        if not container:
            return
        msg=""
        msg += "\nTransfer container's name:\t%s" % (container['transferContainerName'])
        msg += "\nTransfer container's rule:\thttps://cms-rucio-webui.cern.ch/rule?rule_id=%s" % (container['transferRuleID'])
        msg += "\n"
        msg += "\nTips on how to check on Rucio StageOut at https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#Stageout_with_Rucio "
        self.logger.info(msg)

    def printOutputDatasets(self, outputDatasets, includeDASURL=False, container=None):
        """
        Function to print the list of output datasets (with or without the DAS URL).
        """
        if outputDatasets:
            msg = ""
            if includeDASURL:
                for outputDataset in outputDatasets:
                    msg += "\nOutput dataset:\t\t\t%s" % (outputDataset)
                    msg += "\nOutput dataset DAS URL:\t\thttps://cmsweb.cern.ch/das/request?input={0}&instance=prod%2Fphys03".format(quote(outputDataset, ''))
            else:
                extratab = "\t" if len(outputDatasets) == 1 else ""
                msg += "\nOutput dataset%s:\t\t%s%s" % ("s" if len(outputDatasets) > 1 else "", extratab, outputDatasets[0])
                for outputDataset in outputDatasets[1:]:
                    msg += "\n\t\t\t\t%s%s" % (extratab, outputDataset)
            if container:
                msg += "\nPublish container's rule:\thttps://cms-rucio-webui.cern.ch/rule?rule_id=%s" % (container['publishRuleID'])

            self.logger.info(msg)

    def printPublication(self, publicationEnabled, jobsPerStatus, numProbes, numUnpublishable, taskname, user, crabDBInfo, container=None):
        """Print information about the publication of the output files in DBS.
        """
        # Collecting publication information
        pubStatus = {}
        if (publicationEnabled and 'finished' in jobsPerStatus and jobsPerStatus['finished'] > numProbes):
            pubStatus = self.publicationStatus(taskname, user)
        elif not publicationEnabled:
            pubStatus['status'] = {'disabled': []}
        pubInfo = {}
        pubInfo['publication'] = pubStatus.get('status', {})
        pubInfo['publicationFailures'] = pubStatus.get('failure_reasons', {})

        # The output datasets are written into the Task DB by the post-job
        # when uploading the output files metadata.
        outdatasets = literal_eval(getColumn(crabDBInfo, 'tm_output_dataset') if getColumn(crabDBInfo, 'tm_output_dataset') else 'None')
        pubInfo['outdatasets'] = outdatasets
        pubInfo['jobsPerStatus'] = jobsPerStatus

        if 'publication' not in pubInfo:
            return pubStatus
        # If publication was disabled, print a pertinent message and return.
        if 'disabled' in pubInfo['publication']:
            msg = "\nNo publication information (publication has been disabled in the CRAB configuration file)"
            self.logger.info(msg)
            return pubStatus
        # List of output datasets that are going to be (or are already) published. This
        # list is written into the Tasks DB by the post-job when it does the upload of
        # the output files metadata. This means that the list will be empty until one
        # of the post-jobs will finish executing.
        outputDatasets = pubInfo.get('outdatasets')

        # If publication information is not available yet, print a pertinent message
        # (print first the list of output datasets, without the DAS URL) and return.
        if not pubInfo['publication']:
            self.printOutputDatasets(outputDatasets)
            msg = "\nNo publication information available yet"
            self.logger.info(msg)
            return pubStatus
        # Case in which there was an error in retrieving the publication status.
        pubSource = "\n(from CRAB internal bookkeeping in transferdb)"
        if 'error' in pubInfo['publication']:
            msg = "\nPublication status:\t\t%s" % (pubInfo['publication']['error'])
            msg += pubSource
            self.logger.info(msg)
            # Print the output datasets with the corresponding DAS URL.
            self.printOutputDatasets(outputDatasets, includeDASURL=True, container=container)
            return pubStatus
        if pubInfo['publication'] and outputDatasets:
            states = pubInfo['publication']
            # Don't consider publication states for which 0 files are in this state.
            states_tmp = states.copy()
            for s in states:
                if states[s] == 0:
                    del states_tmp[s]
            states = states_tmp.copy()
            # Count the total number of files to publish. For this we count the number of
            # jobs and the number of files to publish per job (which is equal to the number
            # of output datasets produced by the task, because, when multiple EDM files are
            # produced, each EDM file goes into a different output dataset).
            numJobs = sum(pubInfo['jobsPerStatus'].values()) - numProbes - numUnpublishable
            numOutputDatasets = len(outputDatasets)
            numFilesToPublish = numJobs * numOutputDatasets
            # Count how many of these files have not yet been considered for publication.
            states['unsubmitted'] = numFilesToPublish - sum(states.values())
            # Print the publication status.
            statesList = sorted(states)
            msg = "\nPublication status of {0} dataset(s):\t{1}{2}{3}".format(numOutputDatasets, self._printState(statesList[0], 13), self.indentation, \
                                                                              self._percentageString(statesList[0], states[statesList[0]], numFilesToPublish))
            msg += pubSource
            for jobStatus in statesList[1:]:
                if states[jobStatus]:
                    msg += "\n\t\t\t\t\t{0}{1}{2}".format(self._printState(jobStatus, 13), self.indentation, \
                                                          self._percentageString(jobStatus, states[jobStatus], numFilesToPublish))
            self.logger.info(msg)
            # Print the publication errors.
            if pubInfo.get('publicationFailures'):
                msg = "\nPublication error summary:"
                if 'error' in pubInfo['publicationFailures']:
                    msg += "\t%s" % (pubInfo['publicationFailures']['error'])
                elif pubInfo['publicationFailures'].get('result'):
                    ndigits = int(math.ceil(math.log(numFilesToPublish+1, 10)))
                    for failureReason, numFailedFiles in pubInfo['publicationFailures']['result']:
                        msg += ("\n\n\t%" + str(ndigits) + "s files failed to publish with following error message:\n\n\t\t%s") % (numFailedFiles, failureReason)
                self.logger.info(msg)
            # Print the output datasets with the corresponding DAS URL.
            self.printOutputDatasets(outputDatasets, includeDASURL=True, container=container)

        return pubStatus

    def printRucioQuotaInfo(self, user, site):
        if not self.rucio:
            return
        if site == 'T3_CERN_CERNBOX':
            return

        _, isEnough, isQuotaWarning, quota = isEnoughRucioQuota(self.rucio, site)

        totalGB, _, freeGB = quota
        print("You have %d/%d GBytes available as Rucio quota at site %s" % (freeGB, totalGB, site))
        if not isEnough:
            msg = "%sALARM: Not enough space at ASO destination %s" % (colors.RED, colors.NORMAL)
            msg += "\n This very dangerous. Your output stageout will get stuck."
            msg += "\n status information will be stale and could be misleading"
            msg += "\n %sYOU MUST CLEANUP YOUR RUCIO SPACE IMMEDIATELY (remove some rules)%s:" % (colors.RED, colors.NORMAL)
            self.logger.warning(msg)
            return
        if isQoutaWarning:
            msg = "%sWarning%s: " % (colors.RED, colors.NORMAL)
            msg += "This may be not enough for your output, leading to problems. Consider cleaning up"
            self.logger.warning(msg)
        return

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option("--long",
                               dest="long",
                               default=False,
                               action="store_true",
                               help="Print one status line per job.")
        self.parser.add_option("--sort",
                               dest="sort",
                               default=None,
                               help="Sort failed jobs by 'state', 'site', 'runtime', 'memory', 'cpu', 'retries', 'waste' or 'exitcode'.")
        self.parser.add_option("--json",
                               dest="json",
                               default=False,
                               action="store_true",
                               help="Print status results in JSON format.")
        self.parser.add_option("--summary",
                               dest="summary",
                               default=False,
                               action="store_true",
                               help="Print site summary.")
        self.parser.add_option("--verboseErrors",
                               dest="verboseErrors",
                               default=False,
                               action="store_true",
                               help="Expand error summary, showing error messages for all failed jobs.")
        self.parser.add_option("--jobids",
                               dest="jobids",
                               default=None,
                               help="The ids of jobs to print in crab status --long or --sort." + \
                                    " Comma separated list of integers.")


    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.sort is not None:
            sortOpts = ["state", "site", "runtime", "memory", "cpu", "retries", "waste", "exitcode"]
            if self.options.sort not in sortOpts:
                msg = "%sError%s:" % (colors.RED, colors.NORMAL)
                msg += " Only the following values are accepted for --sort option: %s" % (sortOpts)
                raise ConfigurationException(msg)

        if self.options.jobids:
            jobidstuple = validateJobids(self.options.jobids)
            self.jobids = [str(jobid) for (_, jobid) in jobidstuple]

        if self.options.jobids and not (self.options.long or self.options.sort):
            raise ConfigurationException("Parameter --jobids can only be used in combination "
                                         "with --long or --sort options.")

def to_hms(val):
    s = val % 60
    val -= s
    val = (val / 60)
    m = val % 60
    val -= m
    h = val / 60
    return "%d:%02d:%02d" % (h, m, s)
