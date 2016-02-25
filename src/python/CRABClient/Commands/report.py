import os
import json
from ast import literal_eval

from WMCore.DataStructs.LumiList import LumiList
 
import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.ClientUtilities import colors
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.ClientExceptions import RESTCommunicationException, ConfigurationException


class report(SubCommand):
    """
    Get the list of good lumis for your task identified by the -d/--dir option.
    """
    name = 'report'
    shortnames = ['rep']


    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs)

    
    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report'})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        returndict = {}

        publication = dictresult['result'][0]['publication']

        if self.options.recovery == 'notPublished' and not publication:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The option --recovery=%s has been specified" % (self.options.recovery)
            msg += " (which instructs to determine the not processed lumis based on published datasets),"
            msg += " but publication has been disabled in the CRAB configuration."
            raise ConfigurationException(msg)

        onlyDBSSummary = False
        if not dictresult['result'][0]['lumisToProcess'] or not dictresult['result'][0]['runsAndLumis']:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Cannot get all the needed information for the report."
            msg += " Notice, if your task has been submitted more than 30 days ago, then everything has been cleaned."
            self.logger.info(msg)
            if not publication:
                return returndict
            onlyDBSSummary = True

        def _getNumFiles(jobs, fileType):
            files = set()
            for dummy_jobid, reports in jobs.iteritems():
                for rep in reports:
                    if rep['type'] == fileType:
                        # the split is done to remove the jobnumber at the end of the input file lfn
                        files.add('_'.join(rep['lfn'].split('_')[:-1]))
            return len(files)

        def _getNumEvents(jobs, fileType):
            numEvents = 0
            for dummy_jobid, reports in jobs.iteritems():
                for rep in reports:
                    if rep['type'] == fileType:
                        numEvents += rep['events']
            return numEvents

        ## Extract the reports of the input files.
        poolInOnlyRes = {}
        for jobid, reports in dictresult['result'][0]['runsAndLumis'].iteritems():
            poolInOnlyRes[jobid] = [rep for rep in reports if rep['type'] == 'POOLIN']
        
        ## Calculate how many input files have been processed.
        numFilesProcessed = _getNumFiles(dictresult['result'][0]['runsAndLumis'], 'POOLIN')
        returndict['numFilesProcessed'] = numFilesProcessed

        ## Calculate how many events have been read.
        numEventsRead = _getNumEvents(dictresult['result'][0]['runsAndLumis'], 'POOLIN')
        returndict['numEventsRead'] = numEventsRead

        ## Calculate how many events have been written.
        numEventsWritten = {}
        for filetype in ['EDM', 'TFile', 'FAKE']:
            numEventsWritten[filetype] = _getNumEvents(dictresult['result'][0]['runsAndLumis'], filetype)
        returndict['numEventsWritten'] = numEventsWritten

        ## Get the lumis in the input dataset.
        inputDatasetLumis = dictresult['result'][0]['inputDataset']['lumis']
        if not inputDatasetLumis: # for backward compatibility with tasks submitted before the 3.3.1602 release.
            inputDatasetLumis = dictresult['result'][0]['dbsInLumilistNewClientOldTask']
        returndict['inputDatasetLumis'] = inputDatasetLumis

        ## Get the lumis split across files in the input dataset.
        inputDatasetDuplicateLumis = dictresult['result'][0]['inputDataset']['duplicateLumis']
        returndict['inputDatasetDuplicateLumis'] = inputDatasetDuplicateLumis

        ## Get the lumis that the jobs had to process. This must be a subset of input
        ## dataset lumis & lumi-mask.
        lumisToProcessPerJob = dictresult['result'][0]['lumisToProcess']
        lumisToProcess = {}
        for jobid in lumisToProcessPerJob.keys():
            for run, lumiRanges in lumisToProcessPerJob[jobid].iteritems():
                if run not in lumisToProcess:
                    lumisToProcess[run] = []
                for lumiRange in lumiRanges:
                    lumisToProcess[run].extend(range(lumiRange[0], lumiRange[1]+1))
        lumisToProcess = LumiList(runsAndLumis=lumisToProcess).getCompactList()
        returndict['lumisToProcess'] = lumisToProcess

        ## Get the lumis that have been processed.
        processedLumis = BasicJobType.mergeLumis(poolInOnlyRes)
        returndict['processedLumis'] = processedLumis

        ## Get the run-lumi and number of events information about the output datasets.
        outputDatasetsInfo = dictresult['result'][0]['outputDatasets']
        outputDatasetsLumis = {}
        outputDatasetsNumEvents = {}
        if publication:
            for dataset, info in outputDatasetsInfo.iteritems():
                if info['lumis']:
                    outputDatasetsLumis[dataset] = info['lumis']
                outputDatasetsNumEvents[dataset] = info['numEvents']
        returndict['outputDatasetsLumis'] = outputDatasetsLumis
        returndict['outputDatasetsNumEvents'] = outputDatasetsNumEvents
        numOutputDatasets = len(outputDatasetsInfo)

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
        notProcLumisCalcMethMsg  = "The '%s' lumis were calculated as:" % (self.options.recovery)
        if self.options.recovery == 'notFinished':
            notProcessedLumis = BasicJobType.subtractLumis(lumisToProcess, processedLumis)
            notProcLumisCalcMethMsg += " the lumis to process minus the processed lumis"
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
            for jobid, status in dictresult['result'][0]['statusPerJob'].iteritems():
                if status in ['failed']:
                    for run, lumiRanges in lumisToProcessPerJob[jobid].iteritems():
                        if run not in notProcessedLumis:
                            notProcessedLumis[run] = []
                        for lumiRange in lumiRanges:
                            notProcessedLumis[run].extend(range(lumiRange[0], lumiRange[1]+1))
            notProcessedLumis = LumiList(runsAndLumis=notProcessedLumis).getCompactList()
            notProcLumisCalcMethMsg += " the lumis to process by jobs in status 'failed'"
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
                with open(os.path.join(jsonFileDir, 'outputFilesDobleLumis.json'), 'w') as jsonFile:
                    json.dump(outputFilesDuplicateLumis, jsonFile)
                    jsonFile.write("\n")
                    self.logger.info("  %sWarning%s: Duplicate lumis in output files written to outputFilesDobleLumis.json" % (colors.RED, colors.NORMAL))
        ## 2) Then the summary about output datasets in DBS. For this, publication must
        ##    be True and the output files must be publishable.
        if publication and outputDatasetsInfo:
            if onlyDBSSummary:
                msg = "Will provide a short report with information found in DBS."
                self.logger.info(msg)
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
        if inputDatasetLumis or inputDatasetDuplicateLumis or lumisToProcess:
            self.logger.info("Additional report lumi files:")
        if inputDatasetLumis:
            with open(os.path.join(jsonFileDir, 'inputDatasetLumis.json'), 'w') as jsonFile:
                json.dump(inputDatasetLumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Input dataset lumis (from DBS, at task submission time) written to inputDatasetLumis.json")
        if inputDatasetDuplicateLumis:
            with open(os.path.join(jsonFileDir, 'inputDatasetDuplicateLumis.json'), 'w') as jsonFile:
                json.dump(inputDatasetDuplicateLumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Input dataset duplicate lumis (from DBS, at task submission time) written to inputDatasetDuplicateLumis.json")
        if lumisToProcess:
            with open(os.path.join(jsonFileDir, 'lumisToProcess.json'), 'w') as jsonFile:
                json.dump(lumisToProcess, jsonFile)
                jsonFile.write("\n")
                self.logger.info("  Lumis to process written to lumisToProcess.json")
            
        return returndict


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


    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)

        recoveryMethods = ['notFinished', 'notPublished', 'failed']
        if self.options.recovery not in recoveryMethods:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " The --recovery option only accepts the following values: %s" % (recoveryMethods)
            raise ConfigurationException(msg)

