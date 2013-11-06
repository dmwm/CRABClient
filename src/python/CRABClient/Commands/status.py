from __future__ import division # I want floating points
import urllib
import sys

from CRABClient.client_utilities import colors, getUserName
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import MissingOptionException, RESTCommunicationException
from CRABClient import __version__
from RESTInteractions import HTTPRequests

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
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName']})
        dictresult = dictresult['result'][0] #take just the significant part

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}

        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        self.logger.info("Task status:\t\t\t%s" % dictresult['status'])

        def logJDefErr(jdef):
            """Printing job def failures if any"""
            if jdef['jobdefErrors']:
                self.logger.error("%sFailed to inject %s\t%s out of %s:" %(colors.RED, colors.NORMAL,\
                                                                           jdef['failedJobdefs'], jdef['totalJobdefs']))
                for error in jdef['jobdefErrors']:
                    self.logger.info("\t%s" % error)

        #Print the url of the panda monitor
        if dictresult['taskFailureMsg']:
            self.logger.error("%sError during task injection:%s\t%s" % (colors.RED,colors.NORMAL,dictresult['taskFailureMsg']))
            # We might also have more information in the job def errors
            logJDefErr(jdef=dictresult)
        elif self.cachedinfo['RequestName'] == dictresult['jobSetID']:
            # CRAB3-HTCondor
            taskname = urllib.quote(dictresult['jobSetID'])
            self.logger.info("Monitoring URL:\t\t\thttp://glidemon.web.cern.ch/glidemon/jobs.php?taskname=%s" % taskname
        elif dictresult['jobSetID']:
            username = urllib.quote(getUserName(self.voRole, self.voGroup, self.logger))
            self.logger.info("Panda url:\t\t\thttp://pandamon-cms-dev.cern.ch/jobinfo?jobtype=*&jobsetID=%s&prodUserName=%s" % (dictresult['jobSetID'], username))
            # We have cases where the job def errors are there but we have a job def id
            logJDefErr(jdef=dictresult)

        #Print information about jobs
        states = dictresult['jobsPerStatus']
        total = sum( states[st] for st in states )
        frmt = ''
        for status in states:
            frmt += status + ' %s\t' % self._percentageString(states[status], total)
        if frmt:
            self.logger.info('Details:\t\t\t%s' % frmt)

