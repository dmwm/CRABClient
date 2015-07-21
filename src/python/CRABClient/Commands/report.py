import os
import re
import json
from string import upper
from ast import literal_eval

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.Commands.request_type import request_type
from CRABClient.ClientUtilities import getJobTypes, colors
from CRABClient.ClientExceptions import ConfigurationException

class report(SubCommand):
    """
    Get the list of good lumis for your task identified by the -d/--dir option.
    """
    name = 'report'
    shortnames = ['rep']

    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report', 'shortformat': self.usedbs})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        # check if we got the desired results
        if not self.usedbs and not dictresult['result'][0]['runsAndLumis']:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Cannot get the needed information from the CRAB server."
            msg += " Only jobs in 'finished' state and whose outputs have been transferred can be used."
            msg += " Notice, if your task has been submitted more than 30 days ago, then everything has been cleaned."
            msg += " If you published your outputs, you can use --dbs=yes to get some information."
            self.logger.info(msg)
            return dictresult
        elif self.usedbs and not dictresult['result'][0]['dbsInLumilist'] and not dictresult['result'][0]['dbsOutLumilist']:
            msg  = "%sError%s:" % (colors.RED, colors.NORMAL)
            msg += " Cannot get the needed information from DBS."
            msg += " Please check that the output (or input) datasets are not empty, the jobs have finished and the publication has been performed."
            self.logger.info(msg)
            return dictresult

        returndict = {}
        if not self.usedbs:
            ## Get the run-lumi information of the input files from their filemetadatas.
            poolInOnlyRes = {}
            for jobid, val in dictresult['result'][0]['runsAndLumis'].iteritems():
                poolInOnlyRes[jobid] = [f for f in val if f['type'] == 'POOLIN']
            analyzed, diff = BasicJobType.mergeLumis(poolInOnlyRes, dictresult['result'][0]['lumiMask'])
            ## Get the duplicate run-lumis in the output files. Use for this the run-lumi
            ## information of the input files. Why not to use directly the output files?
            ## Because not all types of output files have run-lumi information in their
            ## filemetadata (note: the run-lumi information in the filemetadata is a copy
            ## of the corresponding information in the FJR). For example, output files
            ## produced by TFileService do not have run-lumi information in the FJR. On the
            ## other hand, input files always have run-lumi information in the FJR, which
            ## lists the runs/lumis in the input file that have been processed by the
            ## corresponding job. And of course, the run-lumi information of an output file
            ## produced by job X should be the (set made out of the) union of the run-lumi
            ## information of the input files to job X.
            outputFilesLumiDict = {}
            for jobid, reports in poolInOnlyRes.iteritems():
                lumiDict = {}
                for rep in reports:
                    for run, lumis in literal_eval(rep['runlumi']).iteritems():
                        run = str(run)
                        lumiDict.setdefault(run, []).extend(map(int, lumis))
                for run, lumis in lumiDict.iteritems():
                    outputFilesLumiDict.setdefault(run, []).extend(list(set(lumis)))
            doubleLumis = BasicJobType.getDoubleLumis(outputFilesLumiDict)
            def _getNumFiles(jobs):
                infiles = set()
                for jn, val in jobs.iteritems():
                    for rep in val:
                        # the split is done to remove the jobnumber at the end of the input file lfn
                        infiles.add('_'.join(rep['lfn'].split('_')[:-1]))
                return len(infiles)
            numFilesProcessed = _getNumFiles(poolInOnlyRes)
            self.logger.info("%d file%s been processed" % (numFilesProcessed, " has" if numFilesProcessed == 1 else "s have"))
            def _getNumEvents(jobs, type):
                for jn, val in jobs.iteritems():
                    yield sum([x['events'] for x in val if x['type'] == type])
            numEventsRead = sum(_getNumEvents(dictresult['result'][0]['runsAndLumis'], 'POOLIN'))
            returndict['eventsRead'] = numEventsRead
            self.logger.info("%d event%s been read" % (numEventsRead, " has" if numEventsRead == 1 else "s have"))
            numEventsWritten = sum(_getNumEvents(dictresult['result'][0]['runsAndLumis'], 'EDM'))
            self.logger.info("%d event%s been written" % (numEventsWritten, " has" if numEventsWritten == 1 else "s have"))
        else:
            analyzed, diff = BasicJobType.subtractLumis(dictresult['result'][0]['dbsInLumilist'], dictresult['result'][0]['dbsOutLumilist'])
            doubleLumis = BasicJobType.getDoubleLumis(dictresult['result'][0]['dbsOutLumilist'])
            numFilesProcessed = dictresult['result'][0]['dbsNumFiles']
            self.logger.info("%d file%s been processed" % (numFilesProcessed, " has" if numFilesProcessed == 1 else "s have"))
            numEventsWritten = dictresult['result'][0]['dbsNumEvents']
            self.logger.info("%d event%s been written" % (numEventsWritten, " has" if numEventsWritten == 1 else "s have"))
        returndict['eventsWritten'] = numEventsWritten
        returndict['processedFiles'] = numFilesProcessed
        if self.outdir:
            if not os.path.exists(self.outdir):
                self.logger.info('Creating directory: %s' % self.outdir)
                os.makedirs(self.outdir)
            jsonFileDir = self.outdir
        else:
            jsonFileDir = os.path.join(self.requestarea, 'results')
        if analyzed:
            with open(os.path.join(jsonFileDir, 'lumiSummary.json'), 'w') as jsonFile:
                json.dump(analyzed, jsonFile)
                jsonFile.write("\n")
                self.logger.info("Analyzed luminosity sections written to %s/lumiSummary.json" % jsonFileDir)
                returndict['analyzedLumis'] = analyzed
        if diff:
            with open(os.path.join(jsonFileDir, 'missingLumiSummary.json'), 'w') as jsonFile:
                json.dump(diff, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sWarning%s: Not analyzed luminosity sections written to %s/missingLumiSummary.json" % (colors.RED, colors.NORMAL, jsonFileDir))
                returndict['missingLumis'] = diff 
        if doubleLumis:
            with open(os.path.join(jsonFileDir, 'double.json'), 'w') as jsonFile:
                json.dump(doubleLumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sWarning%s: Double lumis written to %s/double.json" % (colors.RED, colors.NORMAL, jsonFileDir))
                returndict['doubleLumis'] = doubleLumis
            
        return returndict

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option(  "--outputdir",
                                 dest = "outdir",
                                 default = None,
                                 help = "Directory to write JSON summary to." )

        self.parser.add_option( "--dbs",
                                 dest = "usedbs",
                                 default = 'no',
                                 help = "Use information in DBS to build the input lumi lists and the output lumi lists."+\
                                        " Allowed values are yes/no. Default is no." )

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)
        if not re.match('^yes$|^no$', self.options.usedbs):
            raise ConfigurationException("--dbs option only accepts the yes and no values (--dbs=yes or --dbs=no)")
        self.usedbs = 1 if self.options.usedbs == 'yes' else 0

        self.outdir = self.options.outdir
