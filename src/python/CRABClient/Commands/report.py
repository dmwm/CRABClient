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
        if not self.usedbs and not dictresult['result'][0]['runsAndLumis']:
            self.logger.info('%sError%s: Fail in getting the information we need from the CRAB server. Only job that in FINISH state and the ouput has been transfered can be use' % (colors.RED,colors.NORMAL))

            return {'result' : {'status' : 'ERROR' , 'result' : dictresult}}

        elif self.usedbs and not dictresult['result'][0]['dbsInLumilist'] and not dictresult['result'][0]['dbsOutLumilist']:
            self.logger.info('%sError%s: Fail in getting the information we need from DBS. Please check if the output (or input) dataset are not empty and the jobs is FINISH state and the publication'+\
                             ' has been performed' % (colors.RED, colors.NORMAL))

            return {'result' : {'status' : 'ERROR' , 'result' : dictresult}}

        #get the runlumi per each job. runsAndLumis contains the filematadata info per job
        runlumiLists = map(lambda x: literal_eval(x['runlumi']), dictresult['result'][0]['runsAndLumis'].values())

        if not self.usedbs:
            analyzed, diff, doublelumis = BasicJobType.mergeLumis(runlumiLists, dictresult['result'][0]['lumiMask'])
            numFiles = len(reduce(set().union, map(lambda x: literal_eval(x['parents']), dictresult['result'][0]['runsAndLumis'].values())))
            self.logger.info("%d files have been read" % numFiles)
            self.logger.info("%d events have been read" % sum(map(lambda x: x['events'], dictresult['result'][0]['runsAndLumis'].values())))

        else:
            analyzed, diff, doublelumis = BasicJobType.subtractLumis(dictresult['result'][0]['dbsInLumilist'], dictresult['result'][0]['dbsOutLumilist'])
            self.logger.info("%d files have been read" % dictresult['result'][0]['dbsNumFiles'])
            self.logger.info("%d events have been read" % dictresult['result'][0]['dbsNumEvents'])
        returndict = {}
        if self.outdir:
            if not os.path.exists(self.outdir):
                self.logger.info('Creating directory: %s'  % self.outdir)
                os.makedirs(self.outdir)
            jsonFileDir = self.outdir
        else:
            jsonFileDir = os.path.join(self.requestarea, 'results')
        if analyzed:
            with open(os.path.join(jsonFileDir, 'lumiSummary.json'), 'w') as jsonFile:
                json.dump(analyzed, os.path.join(jsonFile))
                jsonFile.write("\n")
                self.logger.info("Analyzed lumi written to %s/lumiSummary.json" % jsonFileDir)
                returndict['analyzed'] = analyzed
        if diff:
            with open(os.path.join(jsonFileDir, 'missingLumiSummary.json'), 'w') as jsonFile:
                json.dump(diff, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sWarning%s: Not Analyzed lumi written to %s/missingLumiSummary.json" % (colors.RED, colors.NORMAL, jsonFileDir))
                returndict['missingLumi']= diff
        if doublelumis:
            with open(os.path.join(jsonFileDir, 'double.json'), 'w') as jsonFile:
                json.dump(doublelumis, jsonFile)
                jsonFile.write("\n")
                self.logger.info("%sWarning%s: Double lumis written to %s/double.json" % (colors.RED, colors.NORMAL, jsonFileDir))
                returndict['doubleLumis'] = doublelumis

        return {'result' : returndict , 'rawdict' : dictresult['result'][0]}

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option(  "--outputdir",
                                 dest = "outdir",
                                 default = None,
                                 help = "Direcotory to write JSON summary to." )

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
