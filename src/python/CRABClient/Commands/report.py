import json
import os
import re
from string import upper
from ast import literal_eval

from CRABClient.Commands.request_type import request_type
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_utilities import getJobTypes, colors
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.client_exceptions import ConfigurationException
from CRABClient import __version__

from RESTInteractions import HTTPRequests

class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """
    name = 'report'
    shortnames = ['rep']

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report', 'shortformat': self.usedbs})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        # check if we got the desired results
        if not dictresult['result'][0]['runsAndLumis'] and self.usedbs=='no':
            self.logger.info('Cannot get the information we need from the CRAB server. Did the jobs finish with transfer performed?')
            return dictresult
        elif not dictresult['result'][0]['dbsInLumilist'] and not dictresult['result'][0]['dbsOutLumilist'] and self.usedbs=='yes':
            self.logger.info('Cannot get the information we need from DBS. Maybe the output (or input) dataset are empty? Are the jobs finished and the publication'+\
                             ' has been performed?')
            return dictresult

        #get the runlumi per each job. runsAndLumis contains the filematadata info per job
        runlumiLists = map(lambda x: literal_eval(x['runlumi']), dictresult['result'][0]['runsAndLumis'].values())
        if self.usedbs=='no':
            analyzed, diff, doublelumis = BasicJobType.mergeLumis(runlumiLists, dictresult['result'][0]['lumiMask'])
            numFiles = len(reduce(set().union, map(lambda x: literal_eval(x['parents']), dictresult['result'][0]['runsAndLumis'].values())))
            self.logger.info("%d files have been read" % numFiles)
            self.logger.info("%d events have been read" % sum(map(lambda x: x['events'], dictresult['result'][0]['runsAndLumis'].values())))
        else:
            analyzed, diff, doublelumis = BasicJobType.subtractLumis(dictresult['result'][0]['dbsInLumilist'], dictresult['result'][0]['dbsOutLumilist'])
            self.logger.info("%d files have been read" % dictresult['result'][0]['dbsNumFiles'])
            self.logger.info("%d events have been read" % dictresult['result'][0]['dbsNumEvents'])

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
                                 help = "Filename to write JSON summary to" )

        self.parser.add_option( "--dbs",
                                 dest = "usedbs",
                                 default = 'yes',
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
