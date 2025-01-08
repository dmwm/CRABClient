"""
    The commands prepares a directory and the relative scripts to execute the jobs locally.
    It can also execute a specific job if the jobid option is passed
"""
import os
import json
import shutil
import tarfile
import tempfile

from ServerUtilities import getColumn, downloadFromS3

from CRABClient.ClientUtilities import execute_command
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ClientException


class preparelocal(SubCommand):
    """ the preparelocal command instance """

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.destination = None #Save the ASO destintion from he DB when we download input files

    def __call__(self):
        #Creating dest directory if needed
        if self.options.destdir is None:
            self.options.destdir = os.path.join(self.requestarea, 'local')
        if not os.path.isdir(self.options.destdir):
            os.makedirs(self.options.destdir)
        self.options.destdir = os.path.abspath(self.options.destdir)
        cwd = os.getcwd()
        try:
            tmpDir = tempfile.mkdtemp()
            os.chdir(tmpDir)

            self.logger.info("Getting input files into tmp dir %s" % tmpDir)
            self.getInputFiles()

            with(open('input_args.json')) as fd:  # this file is created by DagmanCreator in the TW
                inputArgs = json.load(fd)

            if self.options.jobid:
                self.logger.info("Executing job %s locally" % self.options.jobid)
                self.prepareDir(inputArgs, self.options.destdir)
                self.executeTestRun(self.options.destdir, self.options.jobid)
                self.logger.info("Job execution terminated")
            else:
                self.logger.info("Copying and preparing files for local execution in %s" % self.options.destdir)
                self.prepareDir(inputArgs, self.options.destdir)
                self.logger.info("go to that directory IN A CLEAN SHELL and use  'sh run_job.sh NUMJOB' to execute the job")
        finally:
            os.chdir(cwd)
            shutil.rmtree(tmpDir)

        # all methods called before raise if something goes wrong. Getting here means success
        return {'commandStatus': 'SUCCESS'}

    def getInputFiles(self):
        """
        Get the InputFiles.tar.gz and extract the necessary files
        """
        taskname = self.cachedinfo['RequestName']

        #Get task status from the task DB
        self.logger.debug("Getting status from he DB")
        server = self.crabserver
        crabDBInfo, _, _ = server.get(api='task', data={'subresource': 'search', 'workflow': taskname})
        status = getColumn(crabDBInfo, 'tm_task_status')
        self.destination = getColumn(crabDBInfo, 'tm_asyncdest')
        username = getColumn(crabDBInfo, 'tm_username')
        sandboxName = getColumn(crabDBInfo, 'tm_user_sandbox')
        inputsFilename = os.path.join(os.getcwd(), 'InputFiles.tar.gz')

        if not status in ['UPLOADED', 'SUBMITTED']:
            raise ClientException('Can only execute jobs from tasks in status SUBMITTED or UPLOADED. Current status is %s' % status)
        # new way first
        # following try-except can be removed and only the code in the try kept once
        # there are no more tasks wubmitted with TW version v3.241018 or earlier
        try:  # this will fail with old tasks
            inputsFilename = os.path.join(os.getcwd(), 'InputFiles.tar.gz')
            sandboxFilename = os.path.join(os.getcwd(), 'sandbox.tar.gz')
            downloadFromS3(crabserver=self.crabserver, filepath=inputsFilename,
                           objecttype='runtimefiles', taskname=taskname, logger=self.logger)
            downloadFromS3(crabserver=self.crabserver, filepath=sandboxFilename,
                           objecttype='sandbox', logger=self.logger,
                           tarballname=sandboxName, username=username)
            with tarfile.open(inputsFilename) as tf:
                tf.extractall()
        except:
            # old way for taks submitted "some time ago". They should better have bootstrapped
            # so webdir should be defined.
            self.logger.info('Task was submitted with old TaskWorker, fall back to WEB_DIR for tarballs')
            from ServerUtilities import getProxiedWebDir
            from CRABClient.UserUtilities import curlGetFileFromURL
            webdir = getProxiedWebDir(crabserver=self.crabserver, task=taskname,
                                      logFunction=self.logger.debug)
            httpCode = curlGetFileFromURL(webdir + '/InputFiles.tar.gz', inputsFilename, self.proxyfilename,
                                          logger=self.logger)
            if httpCode != 200:
                raise ClientException("Failed to download 'InputFiles.tar.gz' from %s" % webdir)
            with tarfile.open(inputsFilename) as tf:
                tf.extractall()

    def executeTestRun(self, destDir, jobnr):
        """
         Execute a test run calling CMSRunAnalysis.sh
        """
        os.chdir(destDir)
        cmd = 'eval `scram unsetenv -sh`;'\
              ' bash run_job.sh %s' % str(jobnr)
        execute_command(cmd, logger=self.logger, redirect=False)

    def prepareDir(self, inputArgs, targetDir):
        """ Prepare a directory with just the necessary files:
        """

        self.logger.debug("Creating InputArgs.txt file")
        # NEW: change to have one json file for each job with the argument for CMSRunAnalysis.py
        #      then run_job.sh passes it as only argument to CMSRunAnalysis.sh
        #      keys in that json must have same name as destination attributes for options defined
        #      in CMSRunAnalysis.py's parser. We take care here of renaming
        # KEEP also old code for backward compatibility with tasks created with previous TaskWorker version
        # but may also be useful for local submission https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABPrepareLocal
        inputArgsStr = ("-a %(CRAB_Archive)s --sourceURL=%(CRAB_ISB)s"
                        " --jobNumber=%(CRAB_Id)s --cmsswVersion=%(CRAB_JobSW)s --scramArch=%(CRAB_JobArch)s"
                        " --inputFile=%(inputFiles)s --runAndLumis=%(runAndLumiMask)s"
                        "--lheInputFiles=%(lheInputFiles)s"
                        " --firstEvent=%(firstEvent)s --firstLumi=%(firstLumi)s"
                        " --lastEvent=%(lastEvent)s --firstRun=%(firstRun)s "
                        "--seeding=%(seeding)s --scriptExe=%(scriptExe)s "
                        "--eventsPerLumi=%(eventsPerLumi)s --maxRuntime=%(maxRuntime)s"
                        " --scriptArgs=%(scriptArgs)s -o %(CRAB_AdditionalOutputFiles)s\n")
        # remap key in input_args.json to the argument names required by CMSRunAnalysis.py
        # use as : value_of_argument_name = inputArgs[argMap[argument_name]]
        argMap = {
            'archiveJob': 'CRAB_Archive', 'outFiles': 'CRAB_AdditionalOutputFiles',
            'sourceURL': 'CRAB_ISB', 'cmsswVersion': 'CRAB_JobSW',
            'scramArch': 'CRAB_JobArch', 'runAndLumis': 'runAndLumiMask',
            'inputFile' : 'inputFiles', 'lheInputFiles': 'lheInputFiles',
            # the ones below are not changed
            'firstEvent': 'firstEvent', 'firstLumi': 'firstLumi', 'lastEvent': 'lastEvent',
            'firstRun': 'firstRun', 'seeding': 'seeding', 'scriptExe': 'scriptExe',
            'scriptArgs': 'scriptArgs', 'eventsPerLumi': 'eventsPerLumi', 'maxRuntime': 'maxRuntime'
        }

        # create one JSON argument file for each jobId
        jId = 0
        for jobArgs in inputArgs:
            jId +=1
            inputArgsForScript = {}
            for key, value in argMap.items():
                inputArgsForScript[key] = jobArgs[value]
            inputArgsForScript['jobNumber'] = jId
            with open("%s/JobArgs-%s.json" % (targetDir, jId), 'w') as fh:
                json.dump(inputArgsForScript, fh)

        for f in ["gWMS-CMSRunAnalysis.sh", "CMSRunAnalysis.sh", "cmscp.py", "CMSRunAnalysis.tar.gz",
                  "sandbox.tar.gz", "run_and_lumis.tar.gz", "input_files.tar.gz", "Job.submit",
                  "submit_env.sh", "splitting-summary.json", "input_args.json"
                  ]:
                try:  # for backward compatibility with TW v3.241017 where splitting-summary.json is missing
                    shutil.copy2(f, targetDir)
                except FileNotFoundError:
                    pass

        cmd = "cd %s; tar xf CMSRunAnalysis.tar.gz" % targetDir
        execute_command(command=cmd, logger=self.logger)

        # this InputArgs.txt is for backward compatibility with old TW
        # but may also be useful for local submission https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABPrepareLocal
        with open(os.path.join(targetDir, "InputArgs.txt"), "w") as fd:
            for ia in inputArgs:
                fd.write(inputArgsStr % ia)

        self.logger.debug("Creating run_job.sh file")
        #Few observations about the wrapper:
        #All the export are done because normally this env is set by condor (see the Environment classad)
        #Exception is CRAB3_RUNTIME_DEBUG that is set to avoid the dashboard code to blows up since come classad are not there
        #We check the X509_USER_PROXY variable is set  otherwise stageout fails
        #The "tar xzmf CMSRunAnalysis.tar.gz" is needed because in CRAB3_RUNTIME_DEBUG mode the file is not unpacked (why?)
        #Job.submit is also modified to set some things that are condor macro expanded during submission (needed by cmscp)
        bashWrapper = """#!/bin/bash

. ./submit_env.sh && save_env && setup_local_env
#
export _CONDOR_JOB_AD=Job.${1}.submit
# leading '+' signs must be removed to use JDL as classAd file
sed -e 's/^+//' Job.submit > Job.${1}.submit
"""
        if self.options.enableStageout:
            self.logger.debug("Creating jobsubmit fixup files")
            os.makedirs(os.path.join(targetDir, "jobsubmit_fixups"))
            i = 1
            for ia in inputArgs:
                with open(os.path.join(targetDir, "jobsubmit_fixups", "job%s" % i), "w") as fd:
                    fd.write("""CRAB_localOutputFiles = "%(CRAB_localOutputFiles)s"
CRAB_Destination = "%(CRAB_Destination)s"
""" % ia)
                i += 1

            bashWrapper += """if [ ! -f "${X509_USER_PROXY}" ]; then
    echo "X509_USER_PROXY variable does not point to a valid file"
    exit
fi
echo "CRAB_Id = \\\"${1}\\\"" >> Job.${1}.submit
echo 'CRAB_StageoutPolicy = "remote"' >> Job.${1}.submit
echo 'CRAB_AsyncDest = "%s"' >> Job.${1}.submit
echo `grep CRAB_OutTempLFNDir Job.submit | tr -d "+"` >> Job.${1}.submit
echo `grep CRAB_OutLFNDir Job.submit | tr -d "+"` >> Job.${1}.submit
cat jobsubmit_fixups/job${1} >> Job.${1}.submit
""" % self.destination
            bashWrapper += './gWMS-CMSRunAnalysis.sh `sed "${1}q;d" InputArgs.txt`'
        else:
            bashWrapper += '\n./CMSRunAnalysis.sh --json JobArgs-${1}.json'
            #bashWrapper += "echo 'CRAB_TransferOutputs = 0' >> Job.${1}.submit\n"
            #bashWrapper += "echo 'CRAB_SaveLogsFlag = 0' >> Job.${1}.submit\n"

        bashWrapper += "\n"  # add new-line at edn of file for easy-to-read

        with open(os.path.join(targetDir, "run_job.sh"), "w") as fd:
            fd.write(bashWrapper)

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option("--jobid",
                               dest="jobid",
                               default=None,
                               type="int",
                               help="Optional id of the job you want to execute locally")

        self.parser.add_option("--enableStageout",
                               dest="enableStageout",
                               default=False,
                               action="store_true",
                               help="After the job runs copy the output file on the storage destination")

        self.parser.add_option("--destdir",
                               dest="destdir",
                               default=None)

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.jobid is not None:
            try:
                int(self.options.jobid)
            except ValueError:
                raise ClientException("The --jobid option has to be an integer")
