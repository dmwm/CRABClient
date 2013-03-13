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

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}
        listresult = dictresult['result']

        self.logger.info("Task name:\t\t%s" % self.cachedinfo['RequestName'])

        #Print the url of the panda monitor
#        if listresult[Resp.jobDefId]:
#            p = Proxy({'logger' : self.logger})
#            username = urllib.quote(p.getUserName())
#            for jobdefid in listresult[Resp.jobDefId]:
#                self.logger.info("Panda url:\t\thttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%s&user=%s" % (jobdefid, username))

#        if listresult[Resp.workflowErr]:
#            self.logger.error("Workflow has encountered an error: ")
#            for errtype in listresult[Resp.workflowErr]:
#                self.logger.error("\t %s: %s" %(errtype, listresult[Resp.workflowErr][errtype]))

        states = listresult[Resp.jobsPerState]
        total = sum( states[st] for st in states )
        frmt = ''
        resubmissions = 0
        for status in states:
            if states[status] > 0 and status not in ['total', 'first', 'retry']:
                frmt += status + ' %s\t' % self._percentageString(states[status], total)
        if frmt == '' and total != 0:
            frmt = 'jobs are being submitted'
        self.logger.info('Details:\t\t%s' % frmt)

        if listresult[Resp.detailsPerSite]:
            self.logger.info(('Using %d site(s):\t' % len(listresult[Resp.detailsPerSite])) + \
                               ('' if len(listresult[Resp.detailsPerSite])>4 else ', '.join(listresult[Resp.detailsPerSite].keys())))


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
