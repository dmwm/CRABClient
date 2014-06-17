import json
import os
from string import upper
from ast import literal_eval

from CRABClient.Commands.request_type import request_type
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import getJobTypes, colors
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient import __version__
from CRABClient.client_exceptions import RESTCommunicationException
from RESTInteractions import HTTPRequests

class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """
    name = 'report'
    shortnames = ['rep']

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up report for task %s' % self.uniquetaskname)
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.uniquetaskname, 'subresource': 'report'})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.uniquetaskname), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        if not dictresult['result'][0]['runsAndLumis'] :
            self.logger.info('No jobs finished yet. Report is available when jobs complete')
            return

        runlumiLists = map(lambda x: literal_eval(x['runlumi']), dictresult['result'][0]['runsAndLumis'].values())
        #convert lumi lists from strings to integers
        for runlumi in runlumiLists:
            for run in runlumi:
                runlumi[run] = map(int, runlumi[run])
        analyzed, diff, doublelumis = BasicJobType.mergeLumis(runlumiLists, dictresult['result'][0]['lumiMask'])
        numFiles = len(reduce(set().union, map(lambda x: literal_eval(x['parents']), dictresult['result'][0]['runsAndLumis'].values())))
        self.logger.info("%d files have been read" % numFiles)
        self.logger.info("%d events have been read" % sum(map(lambda x: x['events'], dictresult['result'][0]['runsAndLumis'].values())))

        if self.outdir:
            jsonFileDir = self.outdir
        else:
            jsonFileDir = os.path.join(self.requestarea, 'results')
        if analyzed:
            with open(os.path.join(jsonFileDir, 'lumiSummary.json'), 'w') as jsonFile:
                json.dump(analyzed, os.path.join(jsonFile))
                jsonFile.write("\n")
                self.logger.info("Analyzed lumi written to %s/lumiSummary.json" % jsonFileDir)
        if diff:
            with open(os.path.join(jsonFileDir, 'missingLumiSummary.json'), 'w') as jsonFile:
                json.dump(diff, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sNot Analyzed lumi written to %s/missingLumiSummary.json%s" % (colors.RED, jsonFileDir, colors.NORMAL))
        if doublelumis:
            with open(os.path.join(jsonFileDir, 'double.json'), 'w') as jsonFile:
                json.dump(doublelumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sDouble lumis written to %s/double.json%s" % (colors.RED, jsonFileDir, colors.NORMAL))

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-o", "--outputdir",
                                 dest = "outdir",
                                 default = None,
                                 help = "Directory to write JSON summary to" )

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)
        setattr(self, 'outdir', getattr(self.options, 'outdir', None))
