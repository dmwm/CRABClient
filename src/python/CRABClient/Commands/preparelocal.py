import os
import json
import shutil
import tarfile
import tempfile

from ServerUtilities import getProxiedWebDir, getColumn

from CRABClient.UserUtilities import curlGetFileFromURL
from CRABClient.ClientUtilities import execute_command
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ClientException


class preparelocal(SubCommand):
    """ The commands prepare a direcotry and the relative scripts to execute the jobs locally.
        It can also execute a specific job if the jobid option is passed
    """

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

            with(open('input_args.json')) as fd:
                inputArgs = json.load(fd)

            if self.options.jobid:
                self.logger.info("Executing job %s locally" % self.options.jobid)
                self.executeTestRun(inputArgs, self.options.jobid)
                self.logger.info("Job execution terminated")
            else:
                self.logger.info("Copying an preparing files for local execution in %s" % self.options.destdir)
                self.prepareDir(inputArgs, self.options.destdir)
                self.logger.info("go to that directory IN A CLEAN SHELL and use  'sh run_job.sh NUMJOB' to execute the job")
        finally:
            os.chdir(cwd)
            shutil.rmtree(tmpDir)

        return {}

    def getInputFiles(self):
        """ Get the InputFiles.tar.gz and extract the necessary files
        """
        taskname = self.cachedinfo['RequestName']

        #Get task status from the task DB
        self.logger.debug("Getting status from he DB")
        server = self.crabserver
        crabDBInfo, _, _ = server.get(api='task', data={'subresource': 'search', 'workflow': taskname})
        status = getColumn(crabDBInfo, 'tm_task_status')
        self.destination = getColumn(crabDBInfo, 'tm_asyncdest')

        inputsFilename = os.path.join(os.getcwd(), 'InputFiles.tar.gz')
        if status == 'UPLOADED':
            raise ClientException('Currently crab upload only works for tasks successfully submitted')
        elif status == 'SUBMITTED':
            webdir = getProxiedWebDir(crabserver=self.crabserver, task=taskname,
                                      logFunction=self.logger.debug)
            if not webdir:
                webdir = getColumn(crabDBInfo, 'tm_user_webdir')
            self.logger.debug("Downloading 'InputFiles.tar.gz' from %s" % webdir)
            httpCode = curlGetFileFromURL(webdir + '/InputFiles.tar.gz', inputsFilename, self.proxyfilename,
                                          logger=self.logger)
            if httpCode != 200:
                self.logger.errror("Failed to download 'InputFiles.tar.gz' from %s", webdir)
        else:
            raise ClientException('Can only execute jobs from tasks in status SUBMITTED or UPLOADED. Current status is %s' % status)

        for name in [inputsFilename, 'CMSRunAnalysis.tar.gz', 'sandbox.tar.gz']:
            with tarfile.open(name) as tf:
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tf)

    def executeTestRun(self, inputArgs, jobnr):
        """ Execute a test run calling CMSRunAnalysis.sh
        """
        os.environ.update({'CRAB3_RUNTIME_DEBUG': 'True', '_CONDOR_JOB_AD': 'Job.submit'})

        optsList = [
            os.path.join(os.getcwd(), 'TweakPSet.py'),
            '-a %s' % inputArgs[jobnr-1]['CRAB_Archive'],
            '-o %s' % inputArgs[jobnr-1]['CRAB_AdditionalOutputFiles'],
            '--sourceURL=%s' % inputArgs[jobnr-1]['CRAB_ISB'],
            '--location=%s' % os.getcwd(),
            '--inputFile=%s' % inputArgs[jobnr-1]['inputFiles'],
            '--runAndLumis=%s' % inputArgs[jobnr-1]['runAndLumiMask'],
            '--firstEvent=%s' % inputArgs[jobnr-1]['firstEvent'], #jobs goes from 1 to N, inputArgs from 0 to N-1
            '--lastEvent=%s' % inputArgs[jobnr-1]['lastEvent'],
            '--firstLumi=%s' % inputArgs[jobnr-1]['firstLumi'],
            '--firstRun=%s' % inputArgs[jobnr-1]['firstRun'],
            '--seeding=%s' % inputArgs[jobnr-1]['seeding'],
            '--lheInputFiles=%s' % inputArgs[jobnr-1]['lheInputFiles'],
            '--oneEventMode=0',
            '--eventsPerLumi=%s' % inputArgs[jobnr-1]['eventsPerLumi'],
            '--maxRuntime=-1',
            '--jobNumber=%s' % (jobnr-1),
            '--cmsswVersion=%s' % inputArgs[jobnr-1]['CRAB_JobSW'],
            '--scramArch=%s' % inputArgs[jobnr-1]['CRAB_JobArch'],
            '--scriptExe=%s' % inputArgs[jobnr-1]['scriptExe'],
            '--scriptArgs=%s' % inputArgs[jobnr-1]['scriptArgs'],
        ]
        # from a python list to a string which can be used as shell command argument
        opts = ''
        for opt in optsList:
            opts = opts + ' %s'%opt
        command = 'sh CMSRunAnalysis.sh ' + opts
        out, err, returncode = execute_command(command=command)
        self.logger.debug(out)
        self.logger.debug(err)
        if returncode != 0:
            raise ClientException('Failed to execute local test run:\n StdOut: %s\n StdErr: %s' % (out, err))

    def prepareDir(self, inputArgs, targetDir):
        """ Prepare a directry with just the necessary files:
        """

        self.logger.debug("Creating InputArgs.txt file")
        inputArgsStr = "-a %(CRAB_Archive)s --sourceURL=%(CRAB_ISB)s --jobNumber=%(CRAB_Id)s --cmsswVersion=%(CRAB_JobSW)s --scramArch=%(CRAB_JobArch)s --inputFile=%(inputFiles)s --runAndLumis=%(runAndLumiMask)s --lheInputFiles=%(lheInputFiles)s --firstEvent=%(firstEvent)s --firstLumi=%(firstLumi)s --lastEvent=%(lastEvent)s --firstRun=%(firstRun)s --seeding=%(seeding)s --scriptExe=%(scriptExe)s --eventsPerLumi=%(eventsPerLumi)s --maxRuntime=%(maxRuntime)s --scriptArgs=%(scriptArgs)s -o %(CRAB_AdditionalOutputFiles)s\n"
        for f in ["gWMS-CMSRunAnalysis.sh", "CMSRunAnalysis.sh", "cmscp.py", "CMSRunAnalysis.tar.gz",
                  "sandbox.tar.gz", "run_and_lumis.tar.gz", "input_files.tar.gz", "Job.submit"]:
            shutil.copy2(f, targetDir)
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
        bashWrapper = """export SCRAM_ARCH=slc6_amd64_gcc481; export CRAB_RUNTIME_TARBALL=local; export CRAB_TASKMANAGER_TARBALL=local; export _CONDOR_JOB_AD=Job.${1}.submit
export CRAB3_RUNTIME_DEBUG=True
tar xzmf CMSRunAnalysis.tar.gz
cp Job.submit Job.${1}.submit
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
            bashWrapper += './CMSRunAnalysis.sh `sed "${1}q;d" InputArgs.txt`'
            #bashWrapper += "echo 'CRAB_TransferOutputs = 0' >> Job.${1}.submit\n"
            #bashWrapper += "echo 'CRAB_SaveLogsFlag = 0' >> Job.${1}.submit\n"


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
