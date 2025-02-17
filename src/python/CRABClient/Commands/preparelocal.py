# tell pylint to accept some old style which is needed for python2
# pylint: disable=unspecified-encoding, raise-missing-from
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
        inputsFilename = os.path.join(os.getcwd(), 'InputFiles.tar.gz')
        sandboxFilename = os.path.join(os.getcwd(), 'sandbox.tar.gz')
        downloadFromS3(crabserver=self.crabserver, filepath=inputsFilename,
                       objecttype='runtimefiles', taskname=taskname, logger=self.logger)
        downloadFromS3(crabserver=self.crabserver, filepath=sandboxFilename,
                       objecttype='sandbox', logger=self.logger,
                       tarballname=sandboxName, username=username)
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

        self.logger.debug("Creating run_job.sh file")
        # Few observations about the wrapper:
        # All the export are done because normally this env is set by condor (see the Environment classad)
        # Exception is CRAB3_RUNTIME_DEBUG that is set to avoid the dashboard code to blows up since come classad are not there
        # We check the X509_USER_PROXY variable is set  otherwise stageout fails
        # The "tar xzmf CMSRunAnalysis.tar.gz" is needed because in CRAB3_RUNTIME_DEBUG mode the file is not unpacked (why?)
        # Job.submit is also modified to set some things that are condor macro expanded during submission (needed by cmscp)
        bashWrapper = """#!/bin/bash

. ./submit_env.sh && save_env && setup_local_env

export _CONDOR_JOB_AD=Job.${1}.submit
# leading '+' signs must be removed to use JDL as classAd file
sed -e 's/^+//' Job.submit > Job.${1}.submit

./CMSRunAnalysis.sh --jobId ${1}
"""

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

        self.parser.add_option("--destdir",
                               dest="destdir",
                               default=None,
                               help="Optional name of the directory to use, defaults to <projdir>/local")

    def validateOptions(self):
        SubCommand.validateOptions(self)

        if self.options.jobid is not None:
            try:
                int(self.options.jobid)
            except ValueError:
                raise ClientException("The --jobid option has to be an integer")
