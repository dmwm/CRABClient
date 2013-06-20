import json
import os
from string import upper
from ast import literal_eval

from CRABClient.Commands.request_type import request_type
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_utilities import getJobTypes
from CRABClient.JobType.BasicJobType import BasicJobType

class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """
    name = 'report'
    shortnames = ['rep']

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report'})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        if not dictresult['result'][0]['runsAndLumis'] :
            self.logger.info('No jobs finished yet. Report is available when jobs complete')
            return

        runlumiLists = map(lambda x: literal_eval(x['runlumi']), dictresult['result'][0]['runsAndLumis'].values())
        #convert lumi lists from strings to integers
        for runlumi in runlumiLists:
            for run in runlumi:
                runlumi[run] = map(int, runlumi[run])
        analyzed, diff = BasicJobType.mergeLumis(runlumiLists, dictresult['result'][0]['lumiMask'])
        numFiles = len(reduce(set().union, map(lambda x: literal_eval(x['parents']), dictresult['result'][0]['runsAndLumis'].values())))
        self.logger.info("%d files have been read" % numFiles)
        self.logger.info("%d events have been read" % sum(map(lambda x: x['events'], dictresult['result'][0]['runsAndLumis'].values())))

        if self.outdir:
            jsonFileDir = self.outdir
        else:
            jsonFileDir = os.path.join(self.requestarea, 'results')
        if analyzed:
            with open(os.path.join(jsonFileDir, 'analyzed.json'), 'w') as jsonFile:
                json.dump(analyzed, os.path.join(jsonFile))
                jsonFile.write("\n")
                self.logger.info("Analyzed lumi written to %s/analyzed.json" % jsonFileDir)
        if diff:
            with open(os.path.join(jsonFileDir, 'diff.json'), 'w') as jsonFile:
                json.dump(diff, jsonFile)
                jsonFile.write("\n")
                self.logger.info("Not Analyzed lumi written to %s/diff.json" % jsonFileDir)

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-o", "--outputdir",
                                 dest = "outdir",
                                 default = None,
                                 help = "Filename to write JSON summary to" )

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)
        setattr(self, 'outdir', getattr(self.options, 'outdir', None))
