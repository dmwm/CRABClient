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
            self.logger.info(('%sError%s: Cannot get the information we need from the CRAB server.'
                              'Only job that in the FINISH state and the output has been transferred can be used.'
                              'Notice, if your task has been submitted more than 30 days ago, then everything has been cleaned.'
                              'If you published you can use --dbs=yes to get the some information') % (colors.RED,colors.NORMAL))
            return dictresult
        elif self.usedbs and not dictresult['result'][0]['dbsInLumilist'] and not dictresult['result'][0]['dbsOutLumilist']:
            self.logger.info('%sError%s: Cannot get the information we need from DBS. Please check that the output (or input) datasets are not empty, the jobs have finished, and the publication'+\
                             ' has been performed' % (colors.RED, colors.NORMAL))
            return dictresult

        # Keeping only EDM files
        poolInOnlyRes = {}
        for jn, val in dictresult['result'][0]['runsAndLumis'].iteritems():
             poolInOnlyRes[jn] = [f for f in val if f['type'] == 'POOLIN']

        returndict = {}
        if not self.usedbs:
            analyzed, diff, doublelumis = BasicJobType.mergeLumis(poolInOnlyRes, dictresult['result'][0]['lumiMask'])
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
                    yield sum([ x['events'] for x in val if x['type'] == type])
            numEventsRead = sum(_getNumEvents(dictresult['result'][0]['runsAndLumis'], 'POOLIN'))
            returndict['eventsRead'] = numEventsRead
            self.logger.info("%d event%s been read" % (numEventsRead, " has" if numEventsRead == 1 else "s have"))
            numEventsWritten = sum(_getNumEvents(dictresult['result'][0]['runsAndLumis'], 'EDM'))
            self.logger.info("%d event%s been written" % (numEventsWritten, " has" if numEventsWritten == 1 else "s have"))
        else:
            analyzed, diff, doublelumis = BasicJobType.subtractLumis(dictresult['result'][0]['dbsInLumilist'], dictresult['result'][0]['dbsOutLumilist'])
            numFilesProcessed = dictresult['result'][0]['dbsNumFiles']
            self.logger.info("%d file%s been processed" % (numFilesProcessed, " has" if numFilesProcessed == 1 else "s have"))
            numEventsWritten = dictresult['result'][0]['dbsNumEvents']
            self.logger.info("%d event%s been written" % (numEventsWritten, " has" if numEventsWritten == 1 else "s have"))
        returndict['eventsWritten'] = numEventsWritten
        returndict['processedFiles'] = numFilesProcessed
        if self.outdir:
            if not os.path.exists(self.outdir):
                self.logger.info('Creating directory: %s'  % self.outdir)
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
        if doublelumis:
            with open(os.path.join(jsonFileDir, 'double.json'), 'w') as jsonFile:
                json.dump(doublelumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sWarning%s: Double lumis written to %s/double.json" % (colors.RED, colors.NORMAL, jsonFileDir))
                returndict['doubleLumis'] = doublelumis

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
                                 help = "Tell the server to use the information in DBS to build the input lumi lists and the output lumi lists."+\
                                        "Allowed values are yes/no. Default is yes." )

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)
        if not re.match('^yes$|^no$', self.options.usedbs):
            raise ConfigurationException("--dbs option only accepts the yes and no values (--dbs=yes or --dbs=no)")
        self.usedbs = 1 if self.options.usedbs=='yes' else 0

        setattr(self, 'outdir', getattr(self.options, 'outdir', None))
