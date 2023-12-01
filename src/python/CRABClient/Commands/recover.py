import re
import os
import tarfile
import datetime

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
from CRABClient.ClientUtilities import colors
from ServerUtilities import SERVICE_INSTANCES

SPLITTING_RECOVER_LUMIBASED = set(("LumiBased", "Automatic", "EventAwareLumiBased"))
SPLITTING_RECOVER_FILEBASED = set(("FileBased"))

class recover(SubCommand):
    """
    given a taskname, create a new task that process only what the original task
    did not process yet
    """

    name = "recover"
    shortnames = ["rec"]

    def __call__(self):

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

        if self.failingTaskInfo["splitalgo"] in SPLITTING_RECOVER_LUMIBASED:
            retval = self.stepReport()
            if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

            if "recoverLumimaskPath" not in retval:
                return retval

            retval = self.stepSubmitLumiBased(retval["recoverLumimaskPath"])
            if retval["commandStatus"] != "SUCCESS": return self.stepExit(retval)

        elif self.failingTaskInfo["splitalgo"] in SPLITTING_RECOVER_FILEBASED:
            retval = self.stepSubmitFileBased()
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

        return {"commandStatus": "SUCCESS", "init": None }

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
        with SubcommandExecution(self.logger, "remake") as _:
            retval = remakeCmd.remakecache(self.failingTaskName)
        self.logger.debug("stepRemakeAndValidate() - remake, retval: %s", retval)
        self.logger.debug("stepRemakeAndValidate() - remake, after, self.configuration: %s", self.configuration)
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
            return {"commandStatus": "FAILED", "step": "RemakeAndValidate" , "msg": msg }

        self.failingTaskInfo["splitalgo"] = splitalgo
        self.failingTaskInfo["publication"] = True if getColumn(self.failingCrabDBInfo, 'tm_publication') == "T" else False
        self.failingTaskInfo["username"] = getColumn(self.failingCrabDBInfo, 'tm_username')

        self.logger.debug("stepRemakeAndValidate() - failingtaskinfo - %s", self.failingTaskInfo)

        return {"commandStatus": "SUCCESS", "validate": None }

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
        with SubcommandExecution(self.logger, "status") as _:
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

        return retval

    def stepKill(self):
        """
        side effects:
        - kills the original failing task
        """
        ## step2: kill

        # if the task is already killed or about to be killed, do not kill again
        if self.failingTaskStatus["dbStatus"] == "KILLED" or \
            (self.failingTaskStatus["dbStatus"] in ("NEW", "QUEUED") and self.failingTaskStatus["command"] == "KILL"):
            returnDict = {'kill' : 'already killed', 'commandStatus': 'SUCCESS'}
            self.logger.info("step kill - task already killed")
            return returnDict

        # avoid that crab operators kill users tasks by mistake.
        # if the user who is running crab recover differs from the one who submitted the original task,
        # then kill the task only if the option "--forcekill" is used.
        username = getUsername(self.proxyfilename, logger=self.logger)
        if self.failingTaskInfo["username"] != username and not self.options.__dict__["forceKill"]:
            returnDict = {'kill' : 'do not kill task submitted by another user', 'commandStatus': 'FAILED'}
            self.logger.info("step kill - task submitted by another user, will not kill it")
            return returnDict

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
        with SubcommandExecution(self.logger, "kill") as _:
            retval = killCmd()

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

        # check the task status. 
        # it does not make sense to recover a task in COMPLETED
        if not self.failingTaskStatus["status"] in ("SUBMITTED", "FAILED", "FAILED (KILLED)"):
            msg = "In order to recover a task, the combined status of the task needs can not be {}".format(self.failingTaskStatus["status"])
            return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }

        # the status on the db should be submitted or killed. or about to be killed
        if self.failingTaskStatus["dbStatus"] in ("NEW", "QUEUED"):
            if not self.failingTaskStatus["command"] in ("KILL"):
                msg = "In order to recover a task, when the status of the task in the oracle DB is {}, the task command can not be {}"\
                    .format(self.failingTaskStatus["dbStatus"], self.failingTaskStatus["command"])
                return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }
        else:
            if not self.failingTaskStatus["dbStatus"] in ("SUBMITTED", "KILLED"):
                msg = "In order to recover a task, the status of the task in the oracle DB can not be {}"\
                    .format(self.failingTaskStatus["dbStatus"])
                return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }

        # make sure that the jobs ad publications are in a final state.
        # - [x] make sure that there are no ongoing transfers
        #       transfers are accounted in the job status
        # considering as transient: "idle", "running", "transferring", "cooloff", "held"
        terminalStates = set(("finished", "failed", "killed"))
        # python2: need to convert .keys() into a set
        if not set(self.failingTaskStatus["jobsPerStatus"].keys()).issubset(terminalStates):
            msg = "In order to recover a task, all the jobs need to be in a terminal state ({}). You have {}"\
                .format(terminalStates, self.failingTaskStatus["jobsPerStatus"].keys())
            return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }

        # - [x] make sure that there are no ongoing publications
        self.logger.debug("stepCheckKill - publication %s", self.failingTaskStatus["publication"] )
        terminalStatesPub = set(("failed", "done", "not_required", "disabled"))
        if not set(self.failingTaskStatus["publication"].keys()).issubset(terminalStatesPub):
            msg = "In order to recover a task, publication for all the jobs need to be in a terminal state ({}). You have {}"\
                .format(terminalStatesPub, self.failingTaskStatus["publication"].keys())
            return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }

        # - [x] if all jobs failed, then exit. it is better to submit again the task than using crab recover :)
        #       check that "failed" is the only key of the jobsPerStatus dictionary
        if set(self.failingTaskStatus["jobsPerStatus"].keys()) == set(("failed",)):
            msg = "All the jobs of the original task failed. better submitting it again from scratch than recovering it."
            return {"commandStatus": "FAILED", "step": "checkKill" , "msg": msg }

        return {"commandStatus": "SUCCESS", "checkkill": "task can be recovered"}

    def stepReport(self):
        """
        used to compute which lumisections have not been processed by the original task.
        requires TW to have processed lumisection information for submitting the original task.
        it does not support filebased splitting.

        side effects:
        - populates the directory "result" inside the workdir of the original failing task
          with the output of crab report
        """

        failingTaskPublish = getColumn(self.failingCrabDBInfo, 'tm_publication')
        self.logger.debug("stepReport() - tm_publication: %s %s", type(failingTaskPublish), failingTaskPublish)
        # - if the user specified --strategy=notPublished but the original failing task
        #   disabled publishing, then `crab report` fails and raises and exception.
        #   so, we will automatically switch to notFinished and print a warning
        # - assuming "strategy" is always in self.options.__dict__.keys():
        if failingTaskPublish != "T" and self.options.__dict__["strategy"] != "notFinished":
            self.logger.warning("WARNING - crab report - The original task had publication disabled. recovery strategy changed to notFinished")
            self.options.__dict__["strategy"] = "notFinished"

        try:
            os.remove(os.path.join(self.crabProjDir, "results", "notFinishedLumis.json"))
            os.remove(os.path.join(self.crabProjDir, "results", "notPublishedLumis.json"))
            self.logger.info("crab report - needed to delete existing files!")
        except:
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
        with SubcommandExecution(self.logger, "report") as _:
            # FIXME - stays noisy because interference with getMutedStatusInfo()
            retval = reportCmd()
        self.logger.debug("stepReport() - report, after, self.configuration: %s", self.configuration)
        self.logger.debug("stepReport() - report, retval: %s", retval)

        recoverLumimaskPath = ""
        if failingTaskPublish == "T" and self.options.__dict__["strategy"] == "notPublished":
            recoverLumimaskPath = os.path.join(self.crabProjDir, "results", "notPublishedLumis.json")
            # print a proper error message if the original task+recovery task(s) have processed everything.
            publishedAllLumis = True
            for dataset, lumis in retval["outputDatasetsLumis"].items():
                notPublishedLumis = BasicJobType.subtractLumis(retval["lumisToProcess"], lumis )
                self.logger.debug("stepReport() - report, subtract: %s %s", 
                                dataset, notPublishedLumis)
                if notPublishedLumis: 
                    publishedAllLumis = False
            if publishedAllLumis:
                self.logger.info("stepReport() - all lumis have been published in the output dataset. crab recover will exit")
        else:
            if failingTaskPublish == "T" and self.options.__dict__["strategy"] == "notFinished":
                self.logger.warning("%sWarning%s: You are recovering a task with publication enabled with notFinished strategy, this will likely cause to have DUPLICATE LUMIS in the output dataset." % (colors.RED, colors.NORMAL))
            # the only other option should be self.options.__dict__["strategy"] == "notFinished":
            recoverLumimaskPath = os.path.join(self.crabProjDir, "results", "notFinishedLumis.json")
            # print a proper error message if the original task+recovery task(s) have processed everything.
            if not retval["notProcessedLumis"]:
                # we will likely never reach this if, because in this case the status on the schedd
                # should be COMPLETED, which is not accepted by stepCheckKill
                self.logger.info("stepReport() - all lumis have been processed by original task. crab recover will exit")

        self.logger.debug("crab report - recovery task will process lumis contained in file %s", recoverLumimaskPath)


        if os.path.exists(recoverLumimaskPath):
            returnDict = {'commandStatus' : 'SUCCESS', 'recoverLumimaskPath': recoverLumimaskPath}
        else:
            msg = 'the file {} does not exist. crab report could not produce it, the task can not be recovered'.format(recoverLumimaskPath)
            returnDict = {'commandStatus' : 'FAILED', 'msg': msg}

        return returnDict

    def stepGetsandbox(self):
        """
        side effects:
        - download the user_ and debug_sandbox from s3 or from the schedd
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
        self.logger.debug("stepGetsandbox() - cmdargs: %s", cmdargs)
        getsandboxCmd = getsandbox(logger=self.logger, cmdargs=cmdargs)
        with SubcommandExecution(self.logger, "getsandbox") as _:
            retval = getsandboxCmd()
        self.logger.debug("stepGetsandbox() - retval: %s", retval)
        return retval

    def stepExtractSandbox(self, sandbox_paths):
        """
        This step prepares all the information needed for the submit step

        side effects:
        - extracts the user_ and debug_sandbox, so that the files that they contain
          can be used by crab submit at a later step
        """
        debug_sandbox = tarfile.open(sandbox_paths[0])
        debug_sandbox.extractall(path=os.path.join(self.crabProjDir, "user_sandbox"))
        debug_sandbox.close()

        debug_sandbox = tarfile.open(sandbox_paths[1])
        debug_sandbox.extractall(path=os.path.join(self.crabProjDir, "debug_sandbox"))
        debug_sandbox.close()

        self.recoverconfig = os.path.join(self.crabProjDir, "debug_sandbox", 
                                          "debug" , "crabConfig.py")

        return {"commandStatus": "SUCCESS", }

    def stepSubmitLumiBased(self, notFinishedJsonPath):
        """
        Submit a recovery task in the case that the original failing task
        - is of type Analysis
        - used LumiBased splitting algorithm 

        side effect:
        - submits a new task
        """

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
        cmdargs.append("Data.lumiMask={}".format(notFinishedJsonPath))
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

        self.logger.warning("crab submit - recovery task will process lumis contained in file config.Data.lumiMask=%s", notFinishedJsonPath)
        self.logger.debug("stepSubmit() - cmdargs %s", cmdargs)
        submitCmd = submit(logger=self.logger, cmdargs=cmdargs)

        # with SubcommandExecution(self.logger, "submit") as _:
        retval = submitCmd()
        self.logger.debug("stepSubmit() - retval %s", retval)
        return retval

    def stepSubmitFileBased(self):
        """
        Submit a recovery task in the case that the original failing task
        - is of type Analysis
        - used FileBased splitting algorithm 

        what's missing?
        - [ ] if the input is from DBS, then write info to runs_and_lumis.tar.gz
        """

        # TODO
        # I will need to implement this!
        return {'commandStatus': 'FAILED', 'error': 'not implemented yet'}

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
                               help = "When using lumibased splitting, sets crab report --recovery=$option")

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
            regex = "^\d{6}_\d{6}_?([^\:]*)\:[a-zA-Z0-9-]+_(crab_)?.+"
            if not re.match(regex, self.options.cmptask):
                msg = "%sError%s: Task name does not match the regular expression '%s'." % (colors.RED, colors.NORMAL, regex)
                raise ConfigurationException(msg)

        SubCommand.validateOptions(self)

class SubcommandExecution:
    """
    Context manager to silence logging when calling a subcommand.
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
