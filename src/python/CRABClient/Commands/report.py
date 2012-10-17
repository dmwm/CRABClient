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

        reqtype = request_type(self.logger, ['-t', self.options.task, '--proxyfile', self.proxyfilename, '--skip-proxy', '1'])
        requesttype = reqtype()
        jobtypes = getJobTypes()
        plugjobtype = jobtypes[upper(requesttype)](config=None, logger=self.logger, workingdir=None)

        #remove and print output dataset information
        outindexlist = [i for i,el in enumerate(dictresult["result"]) if 'out' in el]
        outi = None
        if len(outindexlist) == 1 and dictresult["result"][outindexlist[0]]['out']:
            outi = outindexlist[0]
            for dataset in dictresult["result"][outi]["out"].keys():
                self.logger.info("Dataset name after publication is: %s " % str(dataset))
                self.logger.info("                       total size: %d Bytes" % dictresult["result"][outi]["out"][dataset].get("size", 0))
                self.logger.info("                     total events: %d " % dictresult["result"][outi]["out"][dataset].get("events", 0))
                self.logger.info("                      total files: %d " % dictresult["result"][outi]["out"][dataset].get("count", 0))
            #this has to be removed to avoid issues with lumi report
            del dictresult["result"][outi]

        howmanylumi, formattedreport = plugjobtype.report(dictresult["result"])
        if outi is not None:
            self.logger.info("            total processed lumis: %d " % howmanylumi)
        else:
            self.logger.info("Processed %d lumis." %howmanylumi)

        if howmanylumi > 0:
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
