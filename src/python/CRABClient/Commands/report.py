import json
import os
from string import upper

from CRABClient.Commands.request_type import request_type
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_utilities import getJobTypes

class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """
    visible = True

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Looking up report for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'subresource': 'report'})

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving report:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        reqtype = request_type(self.logger, ['-t', self.cachedinfo['RequestName'], '--proxyfile', self.proxyfilename, '--skip-proxy', '1'])
        requesttype = reqtype()
        jobtypes = getJobTypes()
        plugjobtype = jobtypes[upper(requesttype)](config=None, logger=self.logger, workingdir=None)

        formattedreport = plugjobtype.report(dictresult["result"])

        if self.outfile:
            jsonFileName = self.outfile
        else:
            jsonFileName = os.path.join(self.requestarea, 'results', 'report.json')
        with open(jsonFileName, 'w') as jsonFile:
            json.dump(formattedreport, jsonFile)
            jsonFile.write("\n")
            self.logger.info("Summary of report written to %s" % jsonFileName)

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-o", "--outputfile",
                                 dest = "outfile",
                                 default = None,
                                 help = "Filename to write JSON summary to" )

    def validateOptions(self):
        """
        Check if the output file is given and set as attribute
        """
        SubCommand.validateOptions(self)
        setattr(self, 'outfile', getattr(self.options, 'outfile', None))
