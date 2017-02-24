from __future__ import print_function, division

import os
import json
import tempfile
import pycurl
import StringIO
import tarfile

import CRABClient.Emulator

from ast import literal_eval

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.pycurl_manager import ResponseHeader

from CRABClient import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.UserUtilities import getMutedStatusInfo
from CRABClient.ClientExceptions import RESTCommunicationException, ConfigurationException, \
    UnknownOptionException, ClientException

from ServerUtilities import FEEDBACKMAIL, getColumn

class report2(SubCommand):
    """
    Important: the __call__ method is almost identical to the old report.

    Get the list of good lumis for your task identified by the -d/--dir option.
    """
    name = 'report2'
    shortnames = ['rep2']

    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)

    def __call__(self):
        reportData = self.collectReportData()

        if not reportData:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Status information is unavailable, will not proceed with the report."
            msg += " Try again a few minutes later if the task has just been submitted."
            self.logger.info(msg)
            return None

        returndict = {}
        if self.options.recovery == 'notPublished' and not reportData['publication']:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The option --recovery=%s has been specified" % (self.options.recovery)
            msg += " (which instructs to determine the not processed lumis based on published datasets),"
            msg += " but publication has been disabled in the CRAB configuration."
            raise ConfigurationException(msg)

        onlyDBSSummary = False
        if not reportData['lumisToProcess'] or not reportData['runsAndLumis']:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Cannot get all the needed information for the report."
            msg += " Notice, if your task has been submitted more than 30 days ago, then everything has been cleaned."
            self.logger.info(msg)
            if not reportData['publication']:
                return returndict
            onlyDBSSummary = True

        def _getNumFiles(jobs, fileType):
            files = set()
            for _, reports in jobs.iteritems():
                for rep in reports:
                    if rep['type'] == fileType:
                        # the split is done to remove the jobnumber at the end of the input file lfn
                        files.add('_'.join(rep['lfn'].split('_')[:-1]))
            return len(files)

        def _getNumEvents(jobs, fileType):
            numEvents = 0
            for _, reports in jobs.iteritems():
                for rep in reports:
                    if rep['type'] == fileType:
                        numEvents += rep['events']
            return numEvents

        ## Extract the reports of the input files.
        poolInOnlyRes = {}
        for jobid, reports in reportData['runsAndLumis'].iteritems():
            poolInOnlyRes[jobid] = [rep for rep in reports if rep['type'] == 'POOLIN']

        ## Calculate how many input files have been processed.
        numFilesProcessed = _getNumFiles(reportData['runsAndLumis'], 'POOLIN')
        returndict['numFilesProcessed'] = numFilesProcessed

        ## Calculate how many events have been read.
        numEventsRead = _getNumEvents(reportData['runsAndLumis'], 'POOLIN')
        returndict['numEventsRead'] = numEventsRead

        ## Calculate how many events have been written.
        numEventsWritten = {}
        for filetype in ['EDM', 'TFile', 'FAKE']:
            numEventsWritten[filetype] = _getNumEvents(reportData['runsAndLumis'], filetype)
        returndict['numEventsWritten'] = numEventsWritten

        ## Get the lumis in the input dataset.
        returndict['inputDatasetLumis'] = reportData['inputDatasetLumis']

        ## Get the lumis split across files in the input dataset.
        returndict['inputDatasetDuplicateLumis'] = reportData['inputDatasetDuplicateLumis']

        ## Get the lumis that the jobs had to process. This must be a subset of input
        ## dataset lumis & lumi-mask.
        lumisToProcessPerJob = reportData['lumisToProcess']
        lumisToProcess = {}
        for jobid in lumisToProcessPerJob.keys():
            for run, lumiRanges in lumisToProcessPerJob[jobid].iteritems():
                if run not in lumisToProcess:
                    lumisToProcess[run] = []
                for lumiRange in lumiRanges:
                    lumisToProcess[run].extend(range(int(lumiRange[0]), int(lumiRange[1])+1))
        lumisToProcess = LumiList(runsAndLumis=lumisToProcess).getCompactList()
        returndict['lumisToProcess'] = lumisToProcess

        ## Get the lumis that have been processed.
        processedLumis = BasicJobType.mergeLumis(poolInOnlyRes)
        returndict['processedLumis'] = processedLumis



        outputDatasetsLumis = {}
        outputDatasetsNumEvents = {}
        if reportData['publication']:
            ## Get the run-lumi and number of events information about the output datasets.
            outputDatasetsInfo = reportData['outputDatasetsInfo']['outputDatasets']
            for dataset in outputDatasetsInfo:
                if outputDatasetsInfo[dataset]['lumis']:
                    outputDatasetsLumis[dataset] = outputDatasetsInfo[dataset]['lumis']
                outputDatasetsNumEvents[dataset] = outputDatasetsInfo[dataset]['numEvents']
        returndict['outputDatasetsLumis'] = outputDatasetsLumis
        returndict['outputDatasetsNumEvents'] = outputDatasetsNumEvents
        numOutputDatasets = len(reportData['outputDatasetsInfo']) if 'outputDatasetsInfo' in reportData else 0


        ## Get the duplicate runs-lumis in the output files. Use for this the run-lumi
        ## information of the input files. Why not to use directly the output files?
        ## Because not all types of output files have run-lumi information in their
        ## filemetadata (note: the run-lumi information in the filemetadata is a copy
        ## of the corresponding information in the FJR). For example, output files
        ## produced by TFileService do not have run-lumi information in the FJR. On the
        ## other hand, input files always have run-lumi information in the FJR, which
        ## lists the runs-lumis in the input file that have been processed by the
        ## corresponding job. And of course, the run-lumi information of an output file
        ## produced by job X should be the (set made out of the) union of the run-lumi
        ## information of the input files to job X.
        outputFilesLumis = {}
        for jobid, reports in poolInOnlyRes.iteritems():
            lumiDict = {}
            for rep in reports:
                for run, lumis in literal_eval(rep['runlumi']).iteritems():
                    lumiDict.setdefault(str(run), []).extend(map(int, lumis))
            for run, lumis in lumiDict.iteritems():
                outputFilesLumis.setdefault(run, []).extend(list(set(lumis)))
        outputFilesDuplicateLumis = BasicJobType.getDuplicateLumis(outputFilesLumis)
        returndict['outputFilesDuplicateLumis'] = outputFilesDuplicateLumis

        ## Calculate the not processed runs-lumis in one of three ways:
        ## 1) The lumis that were supposed to be processed by all jobs minus the lumis
        ##    that were processed by finished (but not necessarily published) jobs.
        ## 2) The lumis that were supposed to be processed by all jobs minus the lumis
        ##    published in all the output datasets.
        ## 3) The lumis that were supposed to be processed by jobs whose status is
        ##    'failed'.
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
            for jobid, status in reportData['jobList']:
                if status in ['failed']:
                    for run, lumiRanges in lumisToProcessPerJob[jobid].iteritems():
                        if run not in notProcessedLumis:
                            notProcessedLumis[run] = []
                        for lumiRange in lumiRanges:
                            notProcessedLumis[run].extend(range(lumiRange[0], lumiRange[1]+1))
            notProcessedLumis = LumiList(runsAndLumis=notProcessedLumis).getCompactList()
            notProcLumisCalcMethMsg += " the lumis to process by jobs in status 'failed'."
        returndict['notProcessedLumis'] = notProcessedLumis

        ## Create the output directory if it doesn't exists.
        if self.options.outdir:
            jsonFileDir = self.options.outdir
        else:
            jsonFileDir = os.path.join(self.requestarea, 'results')
        self.logger.info("Will save lumi files into output directory %s" % (jsonFileDir))
        if not os.path.exists(jsonFileDir):
            self.logger.debug("Creating directory %s" % (jsonFileDir))
            os.makedirs(jsonFileDir)

        ## Create the report JSON files and print a report summary:
        ## 1) First the summary that depends solely on successfully finished jobs (and
        ##    other general information about the task, but not on failed/running jobs).
        if not onlyDBSSummary:
            self.logger.info("Summary from jobs in status 'finished':")
            msg  = "  Number of files processed: %d" % (numFilesProcessed)
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

        ## 2) Then the summary about output datasets in DBS. For this, publication must
        ##    be True and the output files must be publishable.
        if reportData['publication'] and reportData['outputDatasets']:
            if onlyDBSSummary:
                self.logger.info("Will provide a short report with information found in DBS.")
            self.logger.info("Summary from output datasets in DBS:")
            if outputDatasetsNumEvents:
                msg = "  Number of events:"
                for dataset, numEvents in outputDatasetsNumEvents.iteritems():
                    msg += "\n    %s: %d" % (dataset, numEvents)
                self.logger.info(msg)
            if outputDatasetsLumis:
                with open(os.path.join(jsonFileDir, 'outputDatasetsLumis.json'), 'w') as jsonFile:
                    json.dump(outputDatasetsLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  Output datasets lumis written to outputDatasetsLumis.json")
        ## 3) Finally additional files that can be useful for debugging.
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

        return returndict

    def collectReportData(self):
        """
        Gather information from the server, status2, DBS and files in the webdir that is needed for the report.
        """
        reportData = {}

        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])

        # Query server for information from the taskdb, intput/output file metadata from metadatadb
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report2'})

        self.logger.debug("Result: %s" % dictresult)
        self.logger.info("Running crab status2 first to fetch necessary information.")
        # Get job statuses
        crabDBInfo, shortResult = getMutedStatusInfo(self.logger)

        if not shortResult:
            # No point in continuing if the job list is empty.
            # Can happen when the task is very new / old and the files necessary for status2
            # are unavailable.
            return None
        reportData['jobList'] = shortResult['jobList']

        reportData['runsAndLumis'] = {}

        # Transform status joblist (tuples of job status and job id) into a dictionary
        jobStatusDict = {}
        for status, jobId in reportData['jobList']:
            jobStatusDict[jobId] = status

        # Filter output/input file metadata by finished job state
        if dictresult['result'][0]['runsAndLumis']:
            for jobId in jobStatusDict:
                if jobStatusDict.get(jobId) in ['finished']:
                    reportData['runsAndLumis'][jobId] = dictresult['result'][0]['runsAndLumis'][jobId]

        reportData['publication'] = True if getColumn(crabDBInfo, 'tm_publication') == "T" else False
        userWebDirURL = getColumn(crabDBInfo, 'tm_user_webdir')
        numJobs = len(shortResult['jobList'])

        reportData['lumisToProcess'] = self.getLumisToProcess(userWebDirURL, numJobs, self.cachedinfo['RequestName'])
        reportData['inputDataset'] = getColumn(crabDBInfo, 'tm_input_dataset')

        inputDatasetInfo = self.getInputDatasetLumis(reportData['inputDataset'], userWebDirURL)['inputDataset']
        reportData['inputDatasetLumis'] = inputDatasetInfo['lumis']
        reportData['inputDatasetDuplicateLumis'] = inputDatasetInfo['duplicateLumis']
        reportData['outputDatasets'] = dictresult['result'][0]['taskDBInfo']['outputDatasets']

        if reportData['publication']:
            reportData['outputDatasetsInfo'] = self.getDBSPublicationInfo(reportData['outputDatasets'])

        return reportData

    def compactLumis(self, datasetInfo):
        """ Help function that allow to convert from runLumis divided per file (result of listDatasetFileDetails)
            to an aggregated result.
        """
        lumilist = {}
        for _, info in datasetInfo.iteritems():
            for run, lumis in info['Lumis'].iteritems():
                lumilist.setdefault(str(run), []).extend(lumis)
        return lumilist

    def getLumisToProcess(self, userWebDirURL, numJobs, workflow):
        """
        What each job was requested to process

        Get the lumis to process by each job in the workflow.
        """
        res = {}
        res['lumisToProcess'] = {}
        if userWebDirURL:
            curl = self.prepareCurl()
            fp = tempfile.NamedTemporaryFile()
            curl.setopt(pycurl.WRITEFUNCTION, fp.write)
            hbuf = StringIO.StringIO()
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            try:
                url = userWebDirURL + "/run_and_lumis.tar.gz"
                curl.setopt(pycurl.URL, url.encode("ascii", "ignore"))
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    tarball = tarfile.open(fp.name)
                    try:
                        for jobid in xrange(1, numJobs+1):
                            filename = "job_lumis_%d.json" % (jobid)
                            try:
                                member = tarball.getmember(filename)
                            except KeyError:
                                self.logger.warning("File %s not found in run_and_lumis.tar.gz for task %s" % (filename, workflow))
                            else:
                                fd = tarball.extractfile(member)
                                try:
                                    res['lumisToProcess'][str(jobid)] = json.load(fd)
                                finally:
                                    fd.close()
                    finally:
                        tarball.close()
            finally:
                fp.close()
                hbuf.close()
        return res

    def getInputDatasetLumis(self, inputDataset, userWebDirURL):
        """
        What the input dataset had in DBS when the task was submitted

        Get the lumis (and the lumis split across files) in the input dataset. Files
        containing this information were created at data discovery time and then
        copied to the schedd.
        """
        res = {}
        res['inputDataset'] = {'lumis': {}, 'duplicateLumis': {}}
        if inputDataset and userWebDirURL:
            curl = self.prepareCurl()
            fp = tempfile.TemporaryFile()
            curl.setopt(pycurl.WRITEFUNCTION, fp.write)
            hbuf = StringIO.StringIO()
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            try:
                ## Retrieve the lumis in the input dataset.
                url = userWebDirURL + "/input_dataset_lumis.json"
                curl.setopt(pycurl.URL, url.encode("ascii", "ignore"))
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    res['inputDataset']['lumis'] = json.load(fp)
                else:
                    self.logger.error("Failed to retrieve input dataset lumis.")
                ## Clean temp file and buffer.
                fp.seek(0)
                fp.truncate(0)
                hbuf.truncate(0)
                ## Retrieve the lumis split across files in the input dataset.
                url = userWebDirURL + "/input_dataset_duplicate_lumis.json"
                curl.setopt(pycurl.URL, url.encode("ascii", "ignore"))
                self.myPerform(curl, url)
                header = ResponseHeader(hbuf.getvalue())
                if header.status == 200:
                    fp.seek(0)
                    res['inputDataset']['duplicateLumis'] = json.load(fp)
                else:
                    self.logger.error("Failed to retrieve input dataset duplicate lumis.")
            finally:
                fp.close()
                hbuf.close()
        return res

    def getDBSPublicationInfo(self, outputDatasets):
        """
        What has been published

        Get the lumis and number of events in the published output datasets.
        """
        res = {}
        res['outputDatasets'] = {}

        for outputDataset in outputDatasets:
            res['outputDatasets'][outputDataset] = {'lumis': {}, 'numEvents': 0}
            try:
                dbs = DBSReader("https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader",
                                cert=self.proxyfilename, key=self.proxyfilename)
                outputDatasetDetails = dbs.listDatasetFileDetails(outputDataset)
            except Exception as ex:
                msg  = "Failed to retrieve information from DBS for output dataset %s." % (outputDataset)
                msg += " Exception while contacting DBS: %s" % (str(ex))
                self.logger.exception(msg)
            else:
                outputDatasetLumis = self.compactLumis(outputDatasetDetails)
                outputDatasetLumis = LumiList(runsAndLumis=outputDatasetLumis).getCompactList()
                res['outputDatasets'][outputDataset]['lumis'] = outputDatasetLumis
                for outputFileDetails in outputDatasetDetails.values():
                    res['outputDatasets'][outputDataset]['numEvents'] += outputFileDetails['NumberOfEvents']

        return res

    def prepareCurl(self):
        curl = pycurl.Curl()
        curl.setopt(pycurl.NOSIGNAL, 0)
        curl.setopt(pycurl.TIMEOUT, 30)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.FOLLOWLOCATION, 0)
        curl.setopt(pycurl.MAXREDIRS, 0)
        return curl

    def myPerform(self, curl, url):
        try:
            curl.perform()
        except pycurl.error as e:
            raise ClientException(("Failed to contact Grid scheduler when getting URL %s. "
                             "This might be a temporary error, please retry later and "
                             "contact %s if the error persist. Error from curl: %s"
                             % (url, FEEDBACKMAIL, str(e))))
    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option("--outputdir",
                               dest = "outdir",
                               default = None,
                               help = "Directory where to write the lumi summary files.")

        self.parser.add_option("--recovery",
                               dest = "recovery",
                               default = "notFinished",
                               help = "Method to calculate not processed lumis: notFinished," + \
                                      " notPublished or failed [default: %default].")

        self.parser.add_option("--dbs",
                               dest = "usedbs",
                               default = None,
                               help = "Deprecated option removed in CRAB v3.3.1603.")

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
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The --recovery option only accepts the following values: %s" % (recoveryMethods)
            raise ConfigurationException(msg)
