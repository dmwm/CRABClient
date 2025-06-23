# silence pylint complaints about things we need for Python 2.6 compatibility
# pylint: disable=unspecified-encoding, raise-missing-from, consider-using-f-string

import re
import os
import tarfile
import datetime
import json

from CRABClient.Commands.SubCommand import SubCommand

# step: remake
from CRABClient.Commands.remake import remake
from CRABClient.ClientUtilities import colors
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException

# step kill
from CRABClient.Commands.kill import kill
from CRABClient.UserUtilities import getUsername

# step report
from CRABClient.Commands.report import report
from CRABClient.JobType.BasicJobType import BasicJobType

# step status
from CRABClient.Commands.status import status
from CRABClient.ClientUtilities import LOGLEVEL_MUTE

# step getsandbox
from CRABClient.Commands.getsandbox import getsandbox

# step submit
from CRABClient.Commands.submit import submit
from CRABClient.UserUtilities import getColumn
from ServerUtilities import SERVICE_INSTANCES

SPLITTING_RECOVER_LUMIBASED = set(["LumiBased", "Automatic", "EventAwareLumiBased"])
SPLITTING_RECOVER_FILEBASED = set(["FileBased"])

class recover(SubCommand):
    """
    given a taskname, create a new task that processes only what the original task did not process yet
    """

    name = "recover"
    shortnames = ["rec"]

    def __call__(self):
        """
        Code is organized as a series of steps, if any step fails, command exits
        Each step returns a "retval" dictionary which always contains keys: 'commandStatus' and 'step'
          'step' value is the name of the step
          'commandStatus' can be: SUCCESS, FAILED, NothingToDo
        Only the first two can be returned by this method to crab.py, the latter "NothingToDo"
          is used as a "break" to exit the chain of steps early and will be converted to SUCCES before
          this method exits
        Other keys may be present as present in the return dict of subcommands used in here
          if a 'msg' key is present, stepExit will log that message
        """

        retval = self.stepInit()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        if not self.cmdconf["requiresDirOption"]:
            self.failingTaskName = self.options.cmptask
            retval = self.stepRemake()
            if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
            self.crabProjDir = retval["workDir"]
        else:
            self.logger.debug("no need to run crab remake - self.cachedinfo %s", self.cachedinfo)
            self.failingTaskName = self.cachedinfo['RequestName']
            self.restHostCommonname = findServerInstance(self.serverurl, self.instance)
            self.logger.debug("no need to run crab remake - self.serverurl %s", self.serverurl)
            self.logger.debug("no need to run crab remake - self.instance %s", self.instance)
            self.logger.debug("no need to run crab remake - self.restHostCommonname %s", self.restHostCommonname)
            self.crabProjDir = self.requestarea

        self.logger.info("Collecting information about original task ...")

        retval = self.stepValidate()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        retval = self.stepStatus()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        retval = self.stepKill()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
        retval = self.stepCheckKill()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        retval = self.stepGetsandbox()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
        retval = self.stepExtractSandbox(retval["sandbox_paths"])
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        retval = self.stepReport()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
        if "report" not in retval: return self.stepExit(retval)
        report = retval["report"]

        self.logger.info("Prepare recovery task ...")

        retval = self.prepareSubmission()
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
        if "cmdArgs" not in retval: return self.stepExit(retval)
        submitArgs = retval["cmdArgs"]

        if self.failingTaskInfo["splitalgo"] in SPLITTING_RECOVER_LUMIBASED:
            retval = self.stepBuildLumiRecoveryInfo(report)
            if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
            if "recoverLumimaskPath" not in retval: return self.stepExit(retval)
            recoverLumimaskPath = retval["recoverLumimaskPath"]
            submitArgs.append("Data.lumiMask={}".format(recoverLumimaskPath))

        elif self.failingTaskInfo["splitalgo"] in SPLITTING_RECOVER_FILEBASED:
            retval = self.stepBuildFileRecoveryInfo()
            if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
            if "filesToRecover" not in retval: return self.stepExit(retval)
            filesToRecover = retval["filesToRecover"]
            submitArgs.append("Data.userInputFiles={}".format(filesToRecover))

        self.logger.info("Submit recovery task...")

        retval = self.stepSubmit(submitArgs)
        if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        # no need for "else" here, the splitting algo should already be checked in
        # stepRemakeAndValidate

        self.logger.debug("recover - retval %s", retval)
        self.logger.info("crab recover - submitted recovery task %s", retval["uniquerequestname"])
        return retval

    def stepExit(self, retval):
        """
        Callback to be executed after every step executes. 
        Handy if you want to add some logging before the crab recover exits, 
        whatever step the recover fails at.

        Intended to be used as:

        > retval = self.stepYYY()
        > if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)
        """
        if 'msg' in retval:
            self.logger.info("recover process prematurely exited during %s step", retval['step'])
            self.logger.info(retval['msg'])
        if retval['commandStatus'] == 'NothingToDo':
            retval['commandStatus'] = "SUCCESS"  # tell crab.py to exit cleanly with no error
        return retval

    def stepInit(self):
        """
        whatever need to be done before starting with the actual recover process

        - [x] (debug) log the value of some internal variables

        side effects: none
        """
        # self.options and self.args are automatically filled by the __init__()
        # that recover inherits from SubCommand.

        self.logger.debug("stepInit() - self.cmdconf %s", self.cmdconf)
        self.logger.debug("stepInit() - self.cmdargs %s", self.cmdargs)
        self.logger.debug("stepInit() - self.options %s", self.options)
        self.logger.debug("stepInit() - self.args %s",    self.args)

        self.failingTaskStatus = None
        self.failingTaskInfo = {}
        self.failedJobs = []

        return {"commandStatus": "SUCCESS", "step": "init"}

    def stepRemake(self):
        """
        run crab remake then download the info from task DB.
        Use it to perform basic validation of the task that the user wants to recover.
        we support:
        - analysis tasks, not PrivateMC
        - tasks that have been submitted less than 30d ago
            - because crab report needs info from the schedd
        - splitting algorithms based on lumisections and files.

        side effects:
        - if needed, create a new directory locally with requestcache for the 
          original failing task

        TODO an alternative would be to use calling other commands via the crabapi,
        as done with "multicrab".
        https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABClientLibraryAPI#Multicrab_using_the_crabCommand
        """

        # step1: remake
        cmdargs = []
        cmdargs.append("--task")
        cmdargs.append(self.failingTaskName)
        if "instance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["instance"])
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])
        self.logger.debug("stepRemakeAndValidate() - remake, cmdargs: %s", cmdargs)
        remakeCmd = remake(logger=self.logger, cmdargs=cmdargs)
        with SilenceLogging(self.logger, "remake") as _:
            retval = remakeCmd.remakecache(self.failingTaskName)
        self.logger.debug("stepRemakeAndValidate() - remake, retval: %s", retval)
        self.logger.debug("stepRemakeAndValidate() - remake, after, self.configuration: %s", self.configuration)
        retval['step'] = "remake"
        if retval['commandStatus'] != "SUCCESS":
            retval['msg'] = "Could not remake the task project directory"
        return retval

    def stepValidate(self):
        """

        """

        ## validate
        ## - we can not recover a task that is older than 30d, because we need
        ##   files from the schedd about the status of each job
        ## - we want to recover only "analysis" tasks
        self.failingCrabDBInfo, _, _ = self.crabserver.get(api='task', data={'subresource':'search', 'workflow':self.failingTaskName})
        self.logger.debug("stepRemakeAndValidate() - Got information from server oracle database: %s", self.failingCrabDBInfo)
        startTimeDb = getColumn(self.failingCrabDBInfo, 'tm_start_time')
        # 2023-10-24 10:56:26.573303
        # datetime.fromisoformat is not available on py3, we need to use strptime
        # startTime = datetime.datetime.fromisoformat(startTimeDb)
        # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
        startTime = datetime.datetime.strptime(startTimeDb, "%Y-%m-%d %H:%M:%S.%f")
        self.logger.debug("Failing task start time %s %s %s", startTimeDb, startTime, type(startTime))

        if not startTime >= (datetime.datetime.now() - datetime.timedelta(days=30)):
            msg = "The failing task was submitted more than 30d ago. We can not recover it."
            return {"commandStatus": "FAILED", "step": "RemakeAndValidate" , "msg": msg }

        failingJobType = getColumn(self.failingCrabDBInfo, 'tm_job_type')
        if not failingJobType == "Analysis":
            msg = 'crab recover supports only tasks with JobType.pluginName=="Analysis", you  have {}'.format(failingJobType)
            return {"commandStatus": "FAILED", "step": "RemakeAndValidate" , "msg": msg }

        splitalgo = getColumn(self.failingCrabDBInfo, 'tm_split_algo')
        if not splitalgo in SPLITTING_RECOVER_LUMIBASED.union(SPLITTING_RECOVER_FILEBASED):
            msg = 'crab recover supports only tasks with LumiBased and FileBased splitting, you have {}'.format(splitalgo)
            self.logger.info(msg)
            return {"commandStatus": "FAILED", "step": "RemakeAndValidate" , "msg": msg }

        self.failingTaskInfo["splitalgo"] = splitalgo
        self.failingTaskInfo["publication"] = True if getColumn(self.failingCrabDBInfo, 'tm_publication') == "T" else False
        self.failingTaskInfo["username"] = getColumn(self.failingCrabDBInfo, 'tm_username')

        self.logger.debug("stepRemakeAndValidate() - failingtaskinfo - %s", self.failingTaskInfo)

        return {"commandStatus": "SUCCESS", "step": "validate"}

    def stepStatus(self):
        """
        designed for:

        - [x] filebased splitting
        - [x] step check kill

        side effects: none
        """

        cmdargs = []
        cmdargs.append("-d")
        cmdargs.append(str(self.crabProjDir))
        if "instance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["instance"])
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])
        self.logger.debug("stepStatus() - status, cmdargs: %s", cmdargs)
        statusCmd = status(logger=self.logger, cmdargs=cmdargs)
        self.logger.debug("stepStatus() - handlers %s", self.logger.handlers)

        ## old
        # handlerLevels = []
        # for h in self.logger.handlers:
        #     handlerLevels.append(h.level)
        #     h.setLevel(LOGLEVEL_MUTE)
        # retval = statusCmd()
        # self.failingTaskStatus = retval
        # for idx, h in enumerate(self.logger.handlers):
        #     h.setLevel(handlerLevels[idx])
        # self.logger.debug("stepStatus() - handlers %s", self.logger.handlers)
        # self.logger.debug("stepStatus() - handlers %s", handlerLevels)

        ## new
        with SilenceLogging(self.logger, "status") as _:
            retval = statusCmd()
        self.failingTaskStatus = retval

        self.logger.debug("stepStatus() - status, retval: %s", retval)

        ## useful for filebased splitting
        # convert
        # 'jobList': [['failed', '1'], ['failed', '2'], ['finished', '3'], ['failed', '4'], ['finished', '5']]
        # to
        # [1, 2, 4]
        self.failedJobs = [job[1] for job in retval["jobList"] if job[0] == "failed"]
        self.logger.debug("stepStatus() - status, failedJobs: %s", self.failedJobs)
        retval['step'] = "status"
        if retval['commandStatus'] != "SUCCESS":
            retval['msg'] = "Could not retrieve task status"

        return retval

    def stepKill(self):
        """
        side effects:
        - kills the original failing task
        """
        ## step2: kill
        retval = {"step": "kill"}

        # if the task is already killed or about to be killed, do not kill again
        if self.failingTaskStatus["dbStatus"] == "KILLED" or \
            (self.failingTaskStatus["dbStatus"] in ("NEW", "QUEUED") and self.failingTaskStatus["command"] == "KILL"):
            retval['commandStatus'] = "SUCCESS"
            self.logger.debug("step kill - task already killed")
            return retval

        # avoid that crab operators kill users tasks by mistake.
        # if the user who is running crab recover differs from the one who submitted the original task,
        # then kill the task only if the option "--forcekill" is used.
        username = getUsername(self.proxyfilename, logger=self.logger)
        if self.failingTaskInfo["username"] != username and not self.options.__dict__["forceKill"]:
            retval['commandStatus'] = "FAILED"
            retval['msg'] = "task submitted by another user, will not kill it"
            return retval

        cmdargs = []
        cmdargs.append("-d")
        cmdargs.append(str(self.crabProjDir))
        cmdargs.append("--killwarning")
        cmdargs.append("Task killed by crab recover on '{}', by '{}'".format(datetime.datetime.now(), username))
        if "instance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["instance"])
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])
        self.logger.debug("stepKill() - cmdargs: %s", cmdargs)
        killCmd = kill(logger=self.logger, cmdargs=cmdargs)
        with SilenceLogging(self.logger, "kill") as _:
            retval.update(killCmd())

        self.logger.debug("stepKill() - retval: %s", retval)
        self.logger.debug("stepKill() - after, self.configuration: %s", self.configuration)

        return retval

    def stepCheckKill(self):
        """
        make sure that no more files will be produced in the original failing task

        - make sure that the failing task is killed, or about to be killed
            - "about to be killed" means "(new|queued) on command kill"
        - make sure that all jobs are either finished or failed
            - job status comes from status_cache. job+stageout
            - no info about publication
        - TODO make sure that we can identify this also in the case of an autosplitting task

        side effects: none

        ---

        jobsPerStatus can be: 
        - final: finished
        - final: failed
        - final: killed
        - transient: idle
        - transient: running
        - transient: transferring
        - transient: cooloff
        - transient: held TODO 
          jobs should not go into held unless for systemPeriodicHold
          we are not sure if this will ever happen. In order to be safe and cautious,
          we consider this status as transiend and refuse task recovery.
          The user will encounter a problem and contact us

        ## These are all possible statuses of a task in the TaskDB.
        TASKDBSTATUSES_TMP = ['NEW', 'HOLDING', 'QUEUED', 'TAPERECALL', 'KILLRECALL']
        TASKDBSTATUSES_FAILURES = ['SUBMITFAILED', 'KILLFAILED', 'RESUBMITFAILED', 'FAILED']
        TASKDBSTATUSES_FINAL = ['UPLOADED', 'SUBMITTED', 'KILLED'] + TASKDBSTATUSES_FAILURES
        TASKDBSTATUSES = TASKDBSTATUSES_TMP + TASKDBSTATUSES_FINAL
        ## These are all possible statuses of a task as returned by the `status' API.
        TASKSTATUSES = TASKDBSTATUSES + ['COMPLETED', 'UNKNOWN', 'InTransition']

        transfer states:
        TRANSFERDB_STATES = {0: "NEW",
                             1: "ACQUIRED",
                             2: "FAILED",
                             3: "DONE",
                             4: "RETRY",
                             5: "SUBMITTED",
                             6: "KILL",
                             7: "KILLED"}
        publication states:
        PUBLICATIONDB_STATES = {0: "NEW",
                                1: "ACQUIRED",
                                2: "FAILED",
                                3: "DONE",
                                4: "RETRY",
                                5: "NOT_REQUIRED"}
        """

        # make sure the the "task status" is a "static" one
        self.logger.debug("stepCheckKill() - status %s", self.failingTaskStatus["status"])
        self.logger.debug("stepCheckKill() - command %s", self.failingTaskStatus["command"])
        self.logger.debug("stepCheckKill() - dagStatus %s", self.failingTaskStatus["dagStatus"])
        self.logger.debug("stepCheckKill() - dbStatus %s", self.failingTaskStatus["dbStatus"])

        retval = {'step': "checkKill"}

        # check the task status. 
        # it does not make sense to recover a task in COMPLETED
        if not self.failingTaskStatus["status"] in ("SUBMITTED", "FAILED", "FAILED (KILLED)"):
            msg = "Tasks in status {} can not be recovered".format(self.failingTaskStatus["status"])
            retval.update({"commandStatus": "NothingToDo", "msg": msg})
            return retval

        # the status on the db should be submitted or killed. or about to be killed
        if self.failingTaskStatus["dbStatus"] in ("NEW", "QUEUED"):
            if not self.failingTaskStatus["command"] in ("KILL"):
                msg = "In order to recover a task, when the status of the task in the oracle DB is {}, the task command can not be {}"\
                    .format(self.failingTaskStatus["dbStatus"], self.failingTaskStatus["command"])
                retval.update({"commandStatus": "NothingToDo", "msg": msg})
                return retval
        else:
            if not self.failingTaskStatus["dbStatus"] in ("SUBMITTED", "KILLED"):
                msg = "In order to recover a task, the status of the task in the oracle DB can not be {}"\
                    .format(self.failingTaskStatus["dbStatus"])
                retval.update({"commandStatus": "NothingToDo", "msg": msg})
                return retval

        # make sure that the jobs ad publications are in a final state.
        # - [x] make sure that there are no ongoing transfers
        #       transfers are accounted in the job status
        # considering as transient: "idle", "running", "transferring", "cooloff", "held"
        terminalStates = set(("finished", "failed", "killed"))
        # python2: need to convert .keys() into a set
        if not set(self.failingTaskStatus["jobsPerStatus"].keys()).issubset(terminalStates):
            msg = "In order to recover a task, all the jobs need to be in a terminal state ({}). You have {}"\
                .format(terminalStates, self.failingTaskStatus["jobsPerStatus"].keys())
            retval.update({"commandStatus": "NothingToDo", "msg": msg})
            return retval

        # - [x] make sure that there are no ongoing publications
        self.logger.debug("stepCheckKill - publication %s", self.failingTaskStatus["publication"] )
        terminalStatesPub = set(("failed", "done", "not_required", "disabled"))
        if not set(self.failingTaskStatus["publication"].keys()).issubset(terminalStatesPub):
            msg = "In order to recover a task, publication for all the jobs need to be in a terminal state ({}). You have {}"\
                .format(terminalStatesPub, self.failingTaskStatus["publication"].keys())
            retval.update({"commandStatus": "NothingToDo", "msg": msg})
            return retval

        # - [x] if all jobs failed, then exit. it is better to submit again the task than using crab recover :)
        #       check that "failed" is the only key of the jobsPerStatus dictionary
        if set(self.failingTaskStatus["jobsPerStatus"].keys()) == set(("failed",)):
            msg = "All the jobs of the original task failed. Better to investigate and submit it again, rather than recover."
            retval.update({"commandStatus": "NothingToDo", "msg": msg})
            return retval

        retval.update({"commandStatus": "SUCCESS", "msg": "task can be recovered"})
        return retval

    def stepReport(self):
        """
        side effects:
        - populates the directory "result" inside the workdir of the original failing task
          with the output of crab report
        """

        retval = {"step": "report"}
        # - if the user specified --strategy=notPublished but the original failing task
        #   disabled publishing, then `crab report` fails and raises and exception.
        #   so, we will automatically switch to notFinished and print a warning
        # - assuming "strategy" is always in self.options.__dict__.keys():
        if not self.failingTaskInfo["publication"] and self.options.__dict__["strategy"] != "notFinished":
            self.logger.warning("WARNING - crab report - The original task had publication disabled. recovery strategy changed to notFinished")
            self.options.__dict__["strategy"] = "notFinished"

        try:
            os.remove(os.path.join(self.crabProjDir, "results", "notFinishedLumis.json"))
            os.remove(os.path.join(self.crabProjDir, "results", "notPublishedLumis.json"))
            self.logger.info("crab report - needed to delete existing files!")
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        cmdargs = []
        cmdargs.append("-d")
        cmdargs.append(str(self.crabProjDir))
        # if "strategy" in self.options.__dict__.keys():
        cmdargs.append("--recovery")
        cmdargs.append(self.options.__dict__["strategy"])
        if "instance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["instance"])
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])

        self.logger.debug("stepReport() - report, cmdargs: %s", cmdargs)
        reportCmd = report(logger=self.logger, cmdargs=cmdargs)
        with SilenceLogging(self.logger, "report") as _:
            # stays noisy because interference with getMutedStatusInfo()
            retval.update({"report": reportCmd()})
        self.logger.debug("stepReport() - report, after, self.configuration: %s", self.configuration)
        self.logger.debug("stepReport() - report, retval: %s", retval)
        retval.update({'commandStatus' : 'SUCCESS'})
        return retval

    def stepBuildLumiRecoveryInfo(self, report):
        """
        used to compute which lumisections have not been processed by the original task.
        requires TW to have processed lumisection information for submitting the original task.
        returns the path of a file with the lumiMask listing the lumis to recover
        """

        retval = {"step": "buildLumiRecoveryInfo"}
        recoverLumimaskPath = ""
        if self.failingTaskInfo["publication"] and self.options.__dict__["strategy"] == "notPublished":
            recoverLumimaskPath = os.path.join(self.crabProjDir, "results", "notPublishedLumis.json")
            # print a proper error message if the original task+recovery task(s) have processed everything.
            publishedAllLumis = True
            for dataset, lumis in report["outputDatasetsLumis"].items():
                notPublishedLumis = BasicJobType.subtractLumis(report["lumisToProcess"], lumis )
                self.logger.debug("stepBuildLumiReocoveryInfo() - report, subtract: %s %s",
                                dataset, notPublishedLumis)
                if notPublishedLumis: 
                    publishedAllLumis = False
            if publishedAllLumis:
                msg = "stepBuildLumiReocoveryInfo() - all lumis have been published in the output dataset. crab recover will exit"
                self.logger.info(msg)
                retval.update({"commandStatus": "NothingToDo", "msg": msg})
                return retval
        else:
            if self.failingTaskInfo["publication"] and self.options.__dict__["strategy"] == "notFinished":
                self.logger.warning("%sWarning%s: You are recovering a task with publication enabled with notFinished strategy, this will likely cause to have DUPLICATE LUMIS in the output dataset." % (colors.RED, colors.NORMAL))
            # the only other option should be self.options.__dict__["strategy"] == "notFinished":
            recoverLumimaskPath = os.path.join(self.crabProjDir, "results", "notFinishedLumis.json")
            # print a proper error message if the original task+recovery task(s) have processed everything.
            if not retval["notProcessedLumis"]:
                # we will likely never reach this if, because in this case the status on the schedd
                # should be COMPLETED, which is not accepted by stepCheckKill
                self.logger.info("stepBuildLumiReocoveryInfo() - all lumis have been processed by original task. crab recover will exit")
                retval.update({'commandStatus' : 'SUCCESS'})
                return retval

        self.logger.debug("crab report - recovery task will process lumis contained in file %s", recoverLumimaskPath)

        if os.path.exists(recoverLumimaskPath):
            retval.update({'commandStatus' : 'SUCCESS', 'recoverLumimaskPath': recoverLumimaskPath})
        else:
            msg = 'File {} does not exist. crab report could not produce it, the task can not be recovered'.format(recoverLumimaskPath)
            self.logger.info(msg)
            retval.update({'commandStatus' : 'FAILED', 'msg': msg})

        self.logger.warning("recovery task will process lumis contained in file config.Data.lumiMask=%s",
                            recoverLumimaskPath)

        return retval

    def stepBuildFileRecoveryInfo(self):
        """
        returns the list of files to be recovered
        """

        retval = {"step": "buildFileRecoveryInfo"}

        # files to process, processed, failed are in projectDir/results, care of report command
        filesToProcessPath = os.path.join(self.crabProjDir, "results", "filesToProcess.json")
        processedFilesPath = os.path.join(self.crabProjDir, "results", "processedFiles.json")
        with open(filesToProcessPath, 'r') as fd:
            filesToProcessDict = json.load(fd)
        with open(processedFilesPath, 'r') as fd:
            processedFilesDict = json.load(fd)

        # turn {jobid:filelist,..} dictionaries into a simple set
        filesToProcess = set()
        processedFiles = set()
        for job in filesToProcessDict:
            filesToProcess = filesToProcess.union(filesToProcessDict[job])
        for job in processedFilesDict:
            processedFiles = processedFiles.union(processedFilesDict[job])

        # build list of files to recover
        filesToRecover = list(filesToProcess - processedFiles)
        retval.update({'commandStatus' : 'SUCCESS', 'filesToRecover': filesToRecover})

        self.logger.warning("recovery task will process files: %s", filesToRecover)

        return retval

    def stepGetsandbox(self):
        """
        side effects:
        - download the user_ and debug_sandbox from s3 or from the schedd
        """

        retval = {"step": "getSandbox"}

        cmdargs = []
        cmdargs.append("-d")
        cmdargs.append(str(self.crabProjDir))
        if "instance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["instance"])
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])
        self.logger.debug("stepGetsandbox() - cmdargs: %s", cmdargs)
        getsandboxCmd = getsandbox(logger=self.logger, cmdargs=cmdargs)
        with SilenceLogging(self.logger, "getsandbox") as _:
            retval.update(getsandboxCmd())
        self.logger.debug("stepGetsandbox() - retval: %s", retval)
        return retval

    def stepExtractSandbox(self, sandbox_paths):
        """
        This step prepares all the information needed for the submit step

        side effects:
        - extracts the user_ and debug_sandbox, so that the files that they contain
          can be used by crab submit at a later step
        """

        retval = {"step": "extractSandbox"}

        debug_sandbox = tarfile.open(sandbox_paths[0])
        debug_sandbox.extractall(path=os.path.join(self.crabProjDir, "user_sandbox"))
        debug_sandbox.close()

        debug_sandbox = tarfile.open(sandbox_paths[1])
        debug_sandbox.extractall(path=os.path.join(self.crabProjDir, "debug_sandbox"))
        debug_sandbox.close()

        self.recoverconfig = os.path.join(self.crabProjDir, "debug_sandbox", 
                                          "debug" , "crabConfig.py")

        retval.update({"commandStatus": "SUCCESS"})
        return retval

    def prepareSubmission(self):
        """
        returns common command arguments for a new submit command which will do the recover
        """

        retval = {"step": "prepareSubmission"}

        cmdargs = []
        cmdargs.append("-c")
        cmdargs.append(self.recoverconfig)
        if "proxy" in self.options.__dict__.keys():
            cmdargs.append("--proxy")
            cmdargs.append(self.options.__dict__["proxy"])
        if "destinstance" in self.options.__dict__.keys():
            cmdargs.append("--instance")
            cmdargs.append(self.options.__dict__["destinstance"])

        # override config arguments with
        # https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile#Passing_CRAB_configuration_param
        cmdargs.append("General.requestName=None")
        cmdargs.append("General.workArea=.")
        cmdargs.append("JobType.pluginName=Recover")
        cmdargs.append("JobType.copyCatTaskname={}".format(self.failingTaskName))
        cmdargs.append("JobType.copyCatWorkdir={}".format(self.crabProjDir))
        cmdargs.append("JobType.copyCatInstance={}".format(self.restHostCommonname))
        scriptexe = getColumn(self.failingCrabDBInfo, 'tm_scriptexe')
        if scriptexe:
            cmdargs.append("JobType.scriptExe={}".format(os.path.join(self.crabProjDir, "debug_sandbox" , scriptexe)))
        cmdargs.append("JobType.psetName={}".format(os.path.join(self.crabProjDir, "debug_sandbox" , "debug", "originalPSet.py")))

        # when the user running crab recover does not match the user who originally submited the task,
        # then it is likely to be a crab operator, and we should not send filen in the same
        # base dir as the original failing task.
        # we should use default tm_output_lfn when recovering task submitted by another user
        username = getUsername(self.proxyfilename, logger=self.logger)
        if self.failingTaskInfo["username"] != username:
            cmdargs.append("Data.outLFNDirBase=/store/user/{}".format(username))

        retval.update({"commandStatus": "SUCCESS", "cmdArgs": cmdargs})
        return retval

    def stepSubmit(self, cmdargs):
        """
        Submit a recovery task in the case that the original failing task
        - is of type Analysis
        - used FileBased splitting algorithm

        side effect:
        - submits a new task
        """

        retval = {"step": "submit"}

        self.logger.debug("stepSubmit() - cmdargs %s", cmdargs)
        submitCmd = submit(logger=self.logger, cmdargs=cmdargs)
        submitConfiguration = submitCmd.configuration
        workArea = getattr(submitConfiguration.General, 'workArea', '.')

        # submit !
        submitInfo = submitCmd()

        projDir = os.path.join(workArea, submitInfo['requestname'])
        recoveryConfigFilePath = os.path.join(projDir,'inputs/recoveryConfig.py')
        with open(recoveryConfigFilePath, 'w') as fd:
            fd.write(str(submitConfiguration))  # must use a python2-compatible way
        self.logger.info('Submission configuration saved in %s', recoveryConfigFilePath)

        retval.update(submitInfo)

        self.logger.debug("stepSubmit() - retval %s", retval)
        return retval

    def setOptions(self):
        """
        __setOptions__

        """
        # step: remake
        # --dir, --cmptask, --instance already added elsewhere: 

        # step: recovery
        self.parser.add_option("--strategy",
                               dest = "strategy",
                               default="notPublished",
                               help = "When using lumibased splitting, sets crab report --recovery option to this value")

        self.parser.add_option("--destinstance",
                               dest = "destinstance",
                               default = None,
                               help = "(Experimental) The CRABServer instance where you want to submit the recovery task to. Should be used by crab operators only")
        # if the user sets this option, then it gets tricky to setup a link between the original
        # failing task and the recovery task.
        # this option is to be considered experimental and useful for developers only

        # step: kill
        self.parser.add_option("--forcekill",
                        action="store_true", dest="forceKill", default=False,
                        help="Allows to kill failing task submitted by another user. Effective only for crab operators")

    def validateOptions(self):
        """
        __validateOptions__

        """

        # step: remake.
        if self.options.cmptask is None and self.options.projdir is None:
            msg  = "%sError%s: Please specify a CRAB task project directory or the task name for which to remake a CRAB project directory." % (colors.RED, colors.NORMAL)
            msg += " Use the --dir or the --task option."
            ex = MissingOptionException(msg)
            ex.missingOption = "cmptask"
            raise ex
        elif self.options.projdir: 
            self.cmdconf["requiresDirOption"] = True
        elif self.options.cmptask:
            regex = r"^\d{6}_\d{6}_?([^\:]*)\:[a-zA-Z0-9-]+_(crab_)?.+"
            if not re.match(regex, self.options.cmptask):
                msg = "%sError%s: Task name does not match the regular expression '%s'." % (colors.RED, colors.NORMAL, regex)
                raise ConfigurationException(msg)

        SubCommand.validateOptions(self)

class SilenceLogging:
    """
    Context manager to silence logging when e.g. calling a subcommand.
    """

    def __init__(self, logger, commandname):
        self.handlerLevels = []
        self.logger = logger
        self.commandname = commandname

    def __enter__(self):
        self.logger.debug("%s - handlers1: %s", self.commandname, self.logger.handlers)
        for h in self.logger.handlers:
            self.handlerLevels.append(h.level)
            h.setLevel(LOGLEVEL_MUTE)

    def __exit__(self, *exc):
        for idx, h in enumerate(self.logger.handlers):
            h.setLevel(self.handlerLevels[idx])
        self.logger.debug("%s - handlers2: %s", self.commandname, self.handlerLevels)
        self.logger.debug("%s - handlers3: %s", self.commandname, self.logger.handlers)

def findServerInstance(serverurl, dbinstance):
    """
    given ServerUtilities.SERVICE_INSTANCES and (url,db instance) finds the "common" name
    """
    result = None
    for commonName, details in SERVICE_INSTANCES.items():
        if serverurl == details["restHost"] and dbinstance == details["dbInstance"]:
            result = commonName
    return result
