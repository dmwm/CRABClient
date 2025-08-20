# silence pylint complaints about things we need for Python 2.6 compatibility
# pylint: disable=unspecified-encoding, raise-missing-from, consider-using-f-string
# also silence sytle complaints due to use of old code here and there
# pylint: disable=invalid-name

# there are many lines where no LF helps reading, and so do other things
# pylint: disable=multiple-statements, too-many-return-statements, too-many-instance-attributes, too-many-branches


from __future__ import print_function, division

import os
import json
import tempfile
import tarfile
import shutil
from ast import literal_eval

try:
    from FWCore.PythonUtilities.LumiList import LumiList
except Exception:  # pylint: disable=broad-except
    # if FWCore version is not py3 compatible, use our own
    from CRABClient.LumiList import LumiList

from ServerUtilities import downloadFromS3

from CRABClient.ClientUtilities import colors, execute_command
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.UserUtilities import getMutedStatusInfo
from CRABClient.ClientExceptions import (ConfigurationException,
                                         UnknownOptionException, CommandFailedException)

class report(SubCommand):
    """
    Important: the __call__ method is almost identical to the old report.

    Get the list of good lumis for your task identified by the -d/--dir option.
    """
    name = 'report'
    shortnames = ['rep']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)
        self.resultsDir =  os.path.join(self.requestarea, 'results')
        self.taskInfo = {}

    def __call__(self):
        reportData = self.collectReportData()

        if not reportData:
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Status information is unavailable, will not proceed with the report."
            msg += " Try again a few minutes later if the task has just been submitted."
            self.logger.info(msg)
            raise CommandFailedException(msg)

        returndict = {}
        if self.options.recovery == 'notPublished' and not reportData['publication']:
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The option --recovery=%s has been specified" % (self.options.recovery)
            msg += " (which instructs to determine the not processed lumis based on published datasets),"
            msg += " but publication has been disabled in the CRAB configuration."
            raise ConfigurationException(msg)

        if self.options.recovery == 'failed' and self.taskInfo['splitting'] == 'automatic':
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The option --recovery=failed has been specified"
            msg += " but it is not compatible with automatic splitting"
            raise ConfigurationException(msg)

        onlyDBSSummary = False
        if not reportData['lumisToProcess'] or not reportData['processedRunsAndLumis']:
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Cannot get all the needed information for the report. Maybe no job has completed yet ?"
            msg += "\n Notice, if your task has been submitted more than 30 days ago, then everything has been cleaned."
            self.logger.info(msg)
            if not reportData['publication']:
                raise CommandFailedException(msg)
            onlyDBSSummary = True

        def _getNumFiles(jobs, fileType):
            files = set()
            for _, reports in jobs.items():
                for rep in reports:
                    if rep['type'] == fileType:
                        # the split is done to remove the jobnumber at the end of the input file lfn
                        files.add('_'.join(rep['lfn'].split('_')[:-1]))
            return len(files)

        def _getNumEvents(jobs, fileType):
            numEvents = 0
            for _, reports in jobs.items():
                for rep in reports:
                    if rep['type'] == fileType:
                        numEvents += rep['events']
            return numEvents

        # Extract the reports of the input files.
        poolInOnlyRes = {}
        for jobid, reports in reportData['processedRunsAndLumis'].items():
            poolInOnlyRes[jobid] = [rep for rep in reports if rep['type'] == 'POOLIN']

        # Calculate how many input files have been processed.
        numFilesProcessed = _getNumFiles(reportData['processedRunsAndLumis'], 'POOLIN')
        returndict['numFilesProcessed'] = numFilesProcessed

        # Calculate how many events have been read.
        numEventsRead = _getNumEvents(reportData['processedRunsAndLumis'], 'POOLIN')
        returndict['numEventsRead'] = numEventsRead

        # Calculate how many events have been written.
        numEventsWritten = {}
        for filetype in ['EDM', 'TFile', 'FAKE']:
            numEventsWritten[filetype] = _getNumEvents(reportData['processedRunsAndLumis'], filetype)
        returndict['numEventsWritten'] = numEventsWritten

        # Get the lumis in the input dataset.
        returndict['inputDatasetLumis'] = reportData['inputDatasetLumis']

        # Get the lumis split across files in the input dataset.
        returndict['inputDatasetDuplicateLumis'] = reportData['inputDatasetDuplicateLumis']

        # Get the lumis that the jobs had to process. This must be a subset of input
        # dataset lumis & lumi-mask.
        lumisToProcessPerJob = reportData['lumisToProcess']
        lumisToProcess = {}
        for jobid in lumisToProcessPerJob.keys():
            for run, lumiRanges in lumisToProcessPerJob[jobid].items():
                if run not in lumisToProcess:
                    lumisToProcess[run] = []
                for lumiRange in lumiRanges:
                    lumisToProcess[run].extend(range(int(lumiRange[0]), int(lumiRange[1])+1))
        lumisToProcess = LumiList(runsAndLumis=lumisToProcess).getCompactList()
        returndict['lumisToProcess'] = lumisToProcess

        # Get the lumis that have been processed.
        processedLumis = BasicJobType.mergeLumis(poolInOnlyRes)
        returndict['processedLumis'] = processedLumis



        outputDatasetsLumis = {}
        outputDatasetsNumEvents = {}
        if reportData['publication']:
            # Get the run-lumi and number of events information about the output datasets.
            outputDatasetsInfo = reportData['outputDatasetsInfo']['outputDatasets']
            for dataset in outputDatasetsInfo:
                if outputDatasetsInfo[dataset]['lumis']:
                    outputDatasetsLumis[dataset] = outputDatasetsInfo[dataset]['lumis']
                outputDatasetsNumEvents[dataset] = outputDatasetsInfo[dataset]['numEvents']
        returndict['outputDatasetsLumis'] = outputDatasetsLumis
        returndict['outputDatasetsNumEvents'] = outputDatasetsNumEvents
        numOutputDatasets = len(reportData['outputDatasetsInfo']) if 'outputDatasetsInfo' in reportData else 0


        # Get the duplicate runs-lumis in the output files. Use for this the run-lumi
        # information of the input files. Why not to use directly the output files?
        # Because not all types of output files have run-lumi information in their
        # filemetadata (note: the run-lumi information in the filemetadata is a copy
        # of the corresponding information in the FJR). For example, output files
        # produced by TFileService do not have run-lumi information in the FJR. On the
        # other hand, input files always have run-lumi information in the FJR, which
        # lists the runs-lumis in the input file that have been processed by the
        # corresponding job. And of course, the run-lumi information of an output file
        # produced by job X should be the (set made out of the) union of the run-lumi
        # information of the input files to job X.
        outputFilesLumis = {}
        for jobid, reports in poolInOnlyRes.items():
            if jobid.startswith('0-'):  # skip probe-jobs
                continue
            lumiDict = {}
            for rep in reports:
                for run, lumis in literal_eval(rep['runlumi']).items():
                    lumiDict.setdefault(str(run), []).extend(map(int, lumis))
            for run, lumis in lumiDict.items():
                outputFilesLumis.setdefault(run, []).extend(list(set(lumis)))
        outputFilesDuplicateLumis = BasicJobType.getDuplicateLumis(outputFilesLumis)
        returndict['outputFilesDuplicateLumis'] = outputFilesDuplicateLumis

        # Calculate the not processed runs-lumis in one of three ways:
        # 1) The lumis that were supposed to be processed by all jobs minus the lumis
        #    that were processed by finished (but not necessarily published) jobs.
        # 2) The lumis that were supposed to be processed by all jobs minus the lumis
        #    published in all the output datasets.
        # 3) The lumis that were supposed to be processed by jobs whose status is
        #    'failed'.
        notProcessedLumis = {}
        notProcLumisCalcMethMsg = "The '%s' lumis were calculated as:" % (self.options.recovery)
        if self.options.recovery == 'notFinished':
            notProcessedLumis = BasicJobType.subtractLumis(lumisToProcess, processedLumis)
            notProcLumisCalcMethMsg += " the lumis to process minus the processed lumis."
        elif self.options.recovery == 'notPublished':
            publishedLumis = {}
            firstdataset = True
            for dataset in outputDatasetsLumis.keys():
                if firstdataset:
                    publishedLumis = outputDatasetsLumis[dataset]
                    firstdataset = False
                else:
                    publishedLumis = BasicJobType.intersectLumis(publishedLumis, outputDatasetsLumis[dataset])
            notProcessedLumis = BasicJobType.subtractLumis(lumisToProcess, publishedLumis)
            notProcLumisCalcMethMsg += " the lumis to process"
            if numOutputDatasets > 1:
                notProcLumisCalcMethMsg += " minus the lumis published in all the output datasets."
            else:
                notProcLumisCalcMethMsg += " minus the lumis published in the output dataset."
        elif self.options.recovery == 'failed':
            for status, jobid in reportData['jobList']:
                if status in ['failed']:
                    for run, lumiRanges in lumisToProcessPerJob[jobid].items():
                        if run not in notProcessedLumis:
                            notProcessedLumis[run] = []
                        for lumiRange in lumiRanges:
                            notProcessedLumis[run].extend(range(lumiRange[0], lumiRange[1]+1))
            notProcessedLumis = LumiList(runsAndLumis=notProcessedLumis).getCompactList()
            notProcLumisCalcMethMsg += " the lumis to process by jobs in status 'failed'."
        returndict['notProcessedLumis'] = notProcessedLumis

        # Create the output directory if it doesn't exists.
        if self.options.outdir:
            jsonFileDir = self.options.outdir
        else:
            jsonFileDir = self.resultsDir
        self.logger.info("Will put report files in directory %s" % (jsonFileDir))
        if not os.path.exists(jsonFileDir):
            self.logger.debug("Creating directory %s" % (jsonFileDir))
            os.makedirs(jsonFileDir)

        # Create the report JSON files and print a report summary:
        # 1) First the summary that depends solely on successfully finished jobs (and
        #    other general information about the task, but not on failed/running jobs).
        if not onlyDBSSummary:
            self.logger.info("Summary from successful jobs (i.e. in status 'finished'):")
            msg = "  Number of files processed: %d" % (numFilesProcessed)
            msg += "\n  Number of events read: %d" % (numEventsRead)
            msg += "\n  Number of events written in EDM files: %d" % (numEventsWritten.get('EDM', 0))
            msg += "\n  Number of events written in TFileService files: %d" % (numEventsWritten.get('TFile', 0))
            msg += "\n  Number of events written in other type of files: %d" % (numEventsWritten.get('FAKE', 0))
            self.logger.info(msg)
            if processedLumis:
                with open(os.path.join(jsonFileDir, 'processedLumis.json'), 'w') as jsonFile:
                    json.dump(processedLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  Processed lumis written to processedLumis.json")
            if notProcessedLumis:
                filename = self.options.recovery + "Lumis.json"
                with open(os.path.join(jsonFileDir, filename), 'w') as jsonFile:
                    json.dump(notProcessedLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  %sWarning%s: '%s' lumis written to %s" % (colors.RED, colors.NORMAL, self.options.recovery, filename))
                self.logger.info("           %s" % (notProcLumisCalcMethMsg))
            if outputFilesDuplicateLumis:
                with open(os.path.join(jsonFileDir, 'outputFilesDuplicateLumis.json'), 'w') as jsonFile:
                    json.dump(outputFilesDuplicateLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  %sWarning%s: Duplicate lumis in output files written to outputFilesDuplicateLumis.json" % (colors.RED, colors.NORMAL))

        # 2) Then the summary about output datasets in DBS. For this, publication must
        #    be True and the output files must be publishable.
        if reportData['publication'] and reportData['outputDatasets']:
            if onlyDBSSummary:
                self.logger.info("Will provide a short report with information found in DBS.")
            self.logger.info("Summary from output datasets in DBS:")
            if outputDatasetsNumEvents:
                msg = "  Number of events:"
                for dataset, numEvents in outputDatasetsNumEvents.items():
                    msg += "\n    %s: %d" % (dataset, numEvents)
                self.logger.info(msg)
            if outputDatasetsLumis:
                with open(os.path.join(jsonFileDir, 'outputDatasetsLumis.json'), 'w') as jsonFile:
                    json.dump(outputDatasetsLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  Output datasets lumis written to outputDatasetsLumis.json")

        # 3) Then the file summaries
        if reportData['filesToProcess']:
            with open(os.path.join(jsonFileDir, 'filesToProcess.json'), 'w') as jsonFile:
                json.dump(reportData['filesToProcess'], jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Files to process written to filesToProcess.json")
        if reportData['processedFiles']:
            with open(os.path.join(jsonFileDir, 'processedFiles.json'), 'w') as jsonFile:
                json.dump(reportData['processedFiles'], jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Files processed by successful jobs written to processedFiles.json")
        if reportData['failedFiles']:
            with open(os.path.join(jsonFileDir, 'failedFiles.json'), 'w') as jsonFile:
                json.dump(reportData['failedFiles'], jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Files processed by failed jobs written to failedFiles.json")

        # 4) Finally additional files that can be useful for debugging.
        if reportData['inputDatasetLumis'] or reportData['inputDatasetDuplicateLumis'] or lumisToProcess:
            self.logger.info("Additional report lumi files:")
        if reportData['inputDatasetLumis']:
            with open(os.path.join(jsonFileDir, 'inputDatasetLumis.json'), 'w') as jsonFile:
                json.dump(reportData['inputDatasetLumis'], jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Input dataset lumis (from DBS, at task submission time) written to inputDatasetLumis.json")
        if reportData['inputDatasetDuplicateLumis']:
            with open(os.path.join(jsonFileDir, 'inputDatasetDuplicateLumis.json'), 'w') as jsonFile:
                json.dump(reportData['inputDatasetDuplicateLumis'], jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Input dataset duplicate lumis (from DBS, at task submission time) written to inputDatasetDuplicateLumis.json")
        if lumisToProcess:
            with open(os.path.join(jsonFileDir, 'lumisToProcess.json'), 'w') as jsonFile:
                json.dump(lumisToProcess, jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Lumis to process written to lumisToProcess.json")

        # all methods called before raise if something goes wrong. Getting here means success
        returndict['commandStatus'] = 'SUCCESS'
        return returndict

    def collectReportData(self):
        """
        Gather information from the server, status2, DBS and files in S3 that is needed for the report.
        """
        reportData = {}

        server = self.crabserver

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])

        # Query server for information from the taskdb, intput/output file metadata from metadatadb
        dictresult, status, _ = server.get(api=self.defaultApi, data={'workflow': self.cachedinfo['RequestName'], 'subresource': 'report2'})
        self.logger.debug("Result: %s" % dictresult)

        # query more info about task from taskdb
        output, status, _ =  server.get(api='task', data={'workflow': self.cachedinfo['RequestName'], 'subresource': 'status'})
        self.taskInfo = output['result'][0]

        self.logger.info("Running crab status to fetch necessary information.")
        # Get job statuses
        statusDict = getMutedStatusInfo(logger=self.logger, proxy=self.proxyfilename, projdir=self.options.projdir)

        if not statusDict['jobList']:
            # No point in continuing if the job list is empty.
            # Can happen when the task is very new / old and the files necessary for status2
            # are unavailable.
            return None
        reportData['jobList'] = [(s, j) for (s, j) in statusDict['jobList'] if not j.startswith('0-')]

        reportData['processedRunsAndLumis'] = {}
        reportData['processedFiles'] = {}
        reportData['failedFiles'] = {}

        # Transform status joblist (tuples of job status and job id) into a dictionary
        jobStatusDict = {}
        for status, jobId in reportData['jobList']:
            jobStatusDict[jobId] = status

        # Filter output/input file metadata by finished job state
        if dictresult['result'][0]['runsAndLumis']:
            for jobId in jobStatusDict:
                if jobStatusDict.get(jobId) in ['finished']:
                    reportData['processedRunsAndLumis'][jobId] = dictresult['result'][0]['runsAndLumis'][jobId]

        reportData['publication'] = statusDict['publicationEnabled']
        jobs = [j for (s, j) in statusDict['jobList']]

        # download needed files from S3 tarball an place them in result directory
        self.downloadInputFiles(taskname=self.cachedinfo['RequestName'])

        # extract information about what needed to be processed

        (reportData['lumisToProcess'], reportData['filesToProcess'] ) = self.getFilesAndLumisToProcess(jobs)
        reportData['inputDataset'] = statusDict['inputDataset']

        for jobId in jobStatusDict:
            if jobStatusDict[jobId] == 'finished':
                reportData['processedFiles'][jobId] =reportData['filesToProcess'][jobId]
            if jobStatusDict[jobId] == 'failed':
                reportData['failedFiles'][jobId] = reportData['filesToProcess'][jobId]

        inputDatasetInfo = self.getInputDatasetLumis(reportData['inputDataset'])
        reportData['inputDatasetLumis'] = inputDatasetInfo['lumis']
        reportData['inputDatasetDuplicateLumis'] = inputDatasetInfo['duplicateLumis']
        reportData['outputDatasets'] = dictresult['result'][0]['taskDBInfo']['outputDatasets']

        if reportData['publication']:
            repDGO = self.getDBSPublicationInfo_viaDasGoclient(reportData['outputDatasets'])
            reportData['outputDatasetsInfo'] = repDGO

        return reportData

    def downloadInputFiles(self, taskname):
        """
        pulls big tarball from S3 into /tmp and extract in "/results" the files which we need
        """
        tmpDir = tempfile.mkdtemp()
        inputsFilename = os.path.join(tmpDir, 'InputFiles.tar.gz')
        downloadFromS3(crabserver=self.crabserver, filepath=inputsFilename,
                       objecttype='runtimefiles', taskname=taskname, logger=self.logger)
        jobWrapperTarball = os.path.join(tmpDir, 'CMSRunAnalysis.tar.gz')
        twScriptsTarball = os.path.join(tmpDir, 'TaskManagerRun.tar.gz')
        with tarfile.open(inputsFilename) as tf:
            # this contains jobWrapperTarball and twScriptsTarball
            # various needed files are inside those in new version of TW, or present
            # at top level of inputFilename for older TW. Follosing code works for both
            tf.extractall(tmpDir)
        with tarfile.open(jobWrapperTarball) as tf:
            tf.extractall(tmpDir)
        with tarfile.open(twScriptsTarball) as tf:
            tf.extractall(tmpDir)

        shutil.copy2(os.path.join(tmpDir,'input_files.tar.gz'), self.resultsDir)
        shutil.copy2(os.path.join(tmpDir,'run_and_lumis.tar.gz'), self.resultsDir)
        # following files may be missing if input has no dataset info. create empyt JSON in case
        try:
            shutil.copy2(os.path.join(tmpDir, 'input_dataset_lumis.json'), self.resultsDir)
        except FileNotFoundError:
            inputLumisFile =  os.path.join(self.resultsDir, 'input_dataset_lumis.json')
            with open(inputLumisFile, 'w') as fd:
                fd.write('{}')
        try:
            # this one in particular is a old legacy which is likely useless and we
            # may want to remove. Be prepared for it to be missing w/o failing
            shutil.copy2(os.path.join(tmpDir, 'input_dataset_duplicate_lumis.json'), self.resultsDir)
        except FileNotFoundError:
            duplicateLumisFile = os.path.join(self.resultsDir, 'input_dataset_duplicate_lumis.json')
            with open(duplicateLumisFile, 'w') as fd:
                fd.write('{}')
        shutil.rmtree(tmpDir)

    def getFilesAndLumisToProcess(self, jobs):
        """
        What each job was requested to process
        Get the lumis and files to process by each job in the workflow.
        Returns two dictionaries
           {'jobid': {'run':[list of lumi ranges],... } e.g. {"1": [[419, 419]]}
           {'jobid': ['file',...,'file'], ...}
        """
        lumis = {}
        files = {}


        tarFilename = os.path.join(self.resultsDir, 'run_and_lumis.tar.gz')
        with tarfile.open(tarFilename) as tarball:
            for jobid in jobs:
                filename = "job_lumis_%s.json" % (jobid)
                try:
                    member = tarball.getmember(filename)
                except KeyError:
                    self.logger.warning("File %s not found in run_and_lumis.tar.gz" % filename)
                else:
                    fd = tarball.extractfile(member)
                    try:
                        lumis[str(jobid)] = json.load(fd)
                    finally:
                        fd.close()

        tarFilename = os.path.join(self.resultsDir, 'input_files.tar.gz')
        with tarfile.open(tarFilename) as tarball:
            for jobid in jobs:
                filename = "job_input_file_list_%s.txt" % (jobid)
                try:
                    member = tarball.getmember(filename)
                except KeyError:
                    self.logger.warning("File %s not found in input_files.tar.gz" % filename)
                else:
                    fd = tarball.extractfile(member)
                    try:
                        jobFiles = json.load(fd)
                        # inputFile can have three formats depending on wether secondary input files are used:
                        # 1. a single LFN as a string : "/store/.....root"
                        # 2. a list of LFNs : ["/store/.....root", "/store/....root", ...]
                        # 3. a list of dictionaries (one per file) with keys: 'lfn' and 'parents'
                        #   value for 'lfn' is a string, value for 'parents' is a list of {'lfn':lfn} dictionaries
                        #   [{'lfn':inputlfn, 'parents':[{'lfn':parentlfn1},{'lfn':parentlfn2}], ....]},...]
                        if isinstance(jobFiles, str):
                            files[str(jobid)] = [jobFiles]
                        if isinstance(jobFiles, list):
                            files[str(jobid)] = []
                            for f in jobFiles:
                                if isinstance(f, str):
                                    files[str(jobid)].append(f)
                                if isinstance(f, dict):
                                    files[str(jobid)].append(f['lfn'])
                    finally:
                        fd.close()

        return lumis, files

    def getInputDatasetLumis(self, inputDataset):
        """
        What the input dataset had in DBS when the task was submitted

        Get the lumis (and the lumis split across files) in the input dataset. Files
        containing this information were created at data discovery time
        """
        res = {'lumis': {}, 'duplicateLumis': {}}
        if not inputDataset:
            return res

        filename = os.path.join(self.resultsDir, 'input_dataset_lumis.json')
        # Retrieve the lumis in the input dataset.
        with open(filename) as fd:
            res['lumis'] = json.load(fd)
        filename = os.path.join(self.resultsDir, 'input_dataset_duplicate_lumis.json')
        # Retrieve the lumis split across files in the input dataset.
        with open(filename) as fd:
            res['duplicateLumis'] = json.load(fd)
        return res

    def getDBSPublicationInfo_viaDasGoclient(self, outputDatasets):
        """
        reimplemenation of getDBSPublicationInfo with dasgoclient. Remove dependencey from DBS
        Get the lumis and number of events in the published output datasets.
        """
        res = {}
        res['outputDatasets'] = {}
        for outputDataset in outputDatasets:
            res['outputDatasets'][outputDataset] = {'lumis': {}, 'numEvents': 0}
            dbsInstance = "instance=prod/phys03"
            query = "'run,lumi dataset=%s %s'" % (outputDataset, dbsInstance)
            dasgo = "dasgoclient --query " + query + " --json"

            runlumilist = {}
            stdout, stderr, returncode = execute_command(command=dasgo, logger=self.logger)
            if returncode or not stdout:
                self.logger.error('Failed running command %s. Exitcode is %s' % (dasgo, returncode))
                if stdout:
                    self.logger.error('  Stdout:\n    %s' % str(stdout).replace('\n', '\n    '))
                if stderr:
                    self.logger.error('  Stderr:\n    %s' % str(stderr).replace('\n', '\n    '))
            else:
                result = json.loads(stdout)
                for record in result:
                    run = record['run'][0]['run_number']
                    lumis = record['lumi'][0]['number']
                    runlumilist.setdefault(str(run), []).extend(lumis)

            # convert to a LumiList object
            outputDatasetLumis = LumiList(runsAndLumis=runlumilist).getCompactList()
            res['outputDatasets'][outputDataset]['lumis'] = outputDatasetLumis

            # get total events in dataset
            query = "'summary dataset=%s %s'" % (outputDataset, dbsInstance)
            dasgo = "dasgoclient --query " + query + " --json"
            stdout, stderr, returncode = execute_command(command=dasgo, logger=self.logger)
            if returncode or not stdout:
                self.logger.error('Failed running command %s. Exitcode is %s' % (dasgo, returncode))
                if stdout:
                    self.logger.error('  Stdout:\n    %s' % str(stdout).replace('\n', '\n    '))
                if stderr:
                    self.logger.error('  Stderr:\n    %s' % str(stderr).replace('\n', '\n    '))
                total_events = 0
            else:
                result = json.loads(stdout)
                if result:
                    total_events = result[0]['summary'][0]['nevents']
                else:
                    total_events = 0
            res['outputDatasets'][outputDataset]['numEvents'] = total_events

        return res


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option("--outputdir",
                               dest="outdir",
                               default=None,
                               help="Directory where to write the lumi summary files.")

        self.parser.add_option("--recovery",
                               dest="recovery",
                               default="notFinished",
                               help="Strategy to calculate not processed lumis: notFinished," + \
                                      " notPublished or failed [default: %default].")

        self.parser.add_option("--dbs",
                               dest="usedbs",
                               default=None,
                               help="Deprecated option removed in CRAB v3.3.1603.")

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)

        if self.options.usedbs is not None:
            msg = "CRAB command option error: the option --dbs has been deprecated since CRAB v3.3.1603."
            raise UnknownOptionException(msg)

        recoveryMethods = ['notFinished', 'notPublished', 'failed']
        if self.options.recovery not in recoveryMethods:
            msg = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The --recovery option only accepts the following values: %s" % (recoveryMethods)
            raise ConfigurationException(msg)
