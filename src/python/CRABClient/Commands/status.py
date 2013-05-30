from __future__ import division # I want floating points
import urllib

from CRABClient.client_utilities import colors
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import MissingOptionException, RESTCommunicationException

from WMCore.Credential.Proxy import Proxy


class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    shortnames = ['st']
    states = ['submitted', 'failure', 'queued', 'success']

    def _percentageString(self, value, total):
        return "%.2f %% %s(%s/%s)%s" % ((value*100/total), colors.GRAY, value, total, colors.NORMAL)

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName']})
        dictresult = dictresult['result'][0] #take just the significant part

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}

        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        self.logger.info("Task status:\t\t\t%s" % dictresult['status'])

        #Print the url of the panda monitor
        if dictresult['taskFailureMsg']:
            self.logger.error("%sError during task injection:%s\t%s" % (colors.RED,colors.NORMAL,dictresult['taskFailureMsg']))
        elif dictresult['jobSetID']:
            p = Proxy({'logger' : self.logger})
            username = urllib.quote(p.getUserName())
            self.logger.info("Panda url:\t\t\thttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%s&user=%s" % (dictresult['jobSetID'], username))

        if dictresult['jobdefErrors']:
            self.logger.error("%sSubmission partially failed:%s\t%s jobgroup not submittet out of %s:" % (colors.RED, colors.NORMAL,\
                                                            dictresult['failedJobdefs'], dictresult['totalJobdefs']))
            for error in dictresult['jobdefErrors']:
                self.logger.info("\t%s" % error)

        #Print information about jobs
        states = dictresult['jobsPerStatus']
        total = sum( states[st] for st in states )
        frmt = ''
        for status in states:
            frmt += status + ' %s\t' % self._percentageString(states[status], total)
        if frmt:
            self.logger.info('Details:\t\t\t%s' % frmt)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-i", "--site",
                                 dest = "site",
                                 action = "store_true",
                                 default = False,
                                 help = "Provide details about sites" )
