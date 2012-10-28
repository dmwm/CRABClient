from __future__ import division # I want floating points

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import MissingOptionException, RESTCommunicationException

class Resp:
    campaign, campaignStatus, jobsPerState, detailsPerState, detailsPerSite, transferDetails = range(6)

class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    shortnames = ['st']
    complexStates = ['submitted', 'failure', 'queued']
    statesMessage = {'submitted':'', 'failure':'', 'queued':''}

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename)

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName']})

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        #TODO: _printRequestDetails
        self.logger.debug(dictresult)
        listresult = dictresult['result']

        #Getting detailed information if "site" or "failure" options are used.
        #In these cases we need to get the "jobs" information because the "agent" information are not enough
        if self.options.site or self.options.failure:
            errresult, status, reason = server.get('/crabserver/workflow', \
                                data = { 'workflow' : self.cachedinfo['RequestName'], 'subresource' : 'errors', 'shortformat' : 0 if self.options.site else 1})
            if status != 200:
                msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(errresult), str(reason))
                raise RESTCommunicationException(msg)
            self.logger.debug(str(errresult))

        #printing the "task status"
        self.logger.info("Task Status:\t\t%s" % listresult[Resp.campaignStatus])

        #print details about the sites
        if listresult[Resp.detailsPerSite]:
            self.logger.info(('Using %d site(s):\t' % len(listresult[Resp.detailsPerSite])) + \
                               ('' if len(listresult[Resp.detailsPerSite])>4 else ', '.join(listresult[Resp.detailsPerSite].keys())))

        #print the status of the task
        finalMessage = ''
        states = listresult[Resp.jobsPerState]
        total = sum( states[st] for st in states )
        resubmissions = 0
        for status in states:
            line = ''
            detailsLine = ''
            if states[status] > 0:# and status not in ['total', 'first', 'retry']:
                line += '  ' + status + ' %.2f %%\t' % ( states[status]*100/total )

            if status in self.complexStates and status in listresult[Resp.detailsPerState]:
                if 'retry' in listresult[Resp.detailsPerState][status]:
                    resubmissions += listresult[Resp.detailsPerState][status]['retry']

                for detailStatus in listresult[Resp.detailsPerState][status]:
                    if detailStatus not in ['first','retry']:
                        detailsLine += '  ' + detailStatus + ' %.2f %%  ' % ( listresult[Resp.detailsPerState][status][detailStatus]*100/states[status] )

            if line:
                finalMessage += line + ('\t('+self.statesMessage[status]+detailsLine+')\n' if detailsLine else '\n')

        if finalMessage:
            self.logger.info('Jobs Details:\n' + finalMessage[:-1]) #stripping last \n

        if resubmissions:
            self.logger.info('  Using the automatic resubmission (%.2f %%)' % (resubmissions*100/total))

        #print the details about the transfers
        if listresult[Resp.transferDetails]:
            self.logger.info('Further Details:')
            #{u'publication_state': {u'published': 15}, u'state': {u'total': 15, u'done': 15}}
            transfStates = listresult[Resp.transferDetails]['state']
            self.logger.info('  transfers:\t' + (' %.2f %%    '.join(transfStates)+' %.2f %%') % tuple(map(lambda x: x*100/total, transfStates.values())))

            if 'publication_state' in listresult[Resp.transferDetails] and listresult[Resp.transferDetails]['publication_state']:
                publStates = listresult[Resp.transferDetails]['publication_state']
                self.logger.info('  publication:\t' + (' %.2f %%    '.join(publStates)+' %.2f %%') % tuple(map(lambda x: x*100/total, publStates.values())))

        #--site option
        if self.options.site:
            if not listresult[Resp.detailsPerSite]:
                self.logger.info("Information per site are not available.")
            for site in listresult[Resp.detailsPerSite]:
                self.logger.info("%s: " % site)
                states = listresult[Resp.detailsPerSite][site][0]
                line = '    '
                for status in states:
                    if states[status] > 0:
                        line += status + ' %.2f %%\t' % ( states[status]*100/total )
                self.logger.info(line)
                self._printSiteErrors(errresult, site, total)


        #--failure option
        #XXX: The exit code here is the one generated by Report.getExitCode and is not necessarily the CMSSWException one
        if self.options.failure and total:
            self.logger.info("List of jobs errors:")
            for err in errresult['result'][-1]['jobs']:
                self.logger.info("  %.2f %% have exit code %s" % (err['value']*100/total, err['key'][2]))
            if 'state' in errresult['result'][-1]['transfers'] and 'failures_reasons' in errresult['result'][-1]['transfers']:
                self.logger.info("List of transfer errors:")
                #total_transf = sum(errresult['result'][0]['transfers']['state'].values())
                for err, num in errresult['result'][-1]['transfers']['failures_reasons'].items():
                    self.logger.info("  %.2f %% have error %s" % (num/total * 100, err))

    def _printSiteErrors(self, errresult, site, total):
        """
        Print the error details of the site (when option -i is used)
        """
        _, _, EXITCODE, SITE, ERRLIST = range(5)
        if 'result' in errresult and len(errresult['result'])>0:
            for row in errresult['result'][-1]['jobs']:
                if row['key'][SITE] == site:
                     self.logger.info("    %.2f %% with exit code %s. Error list: %s" % (row['value']*100/total, row['key'][EXITCODE], row['key'][ERRLIST]))

    def _printRequestDetails(self, dictresult):
        """
        Print the RequestMessages list when the task is failed
        """
        if dictresult.has_key('requestDetails') and \
                  dictresult['requestDetails'][u'RequestStatus'] == 'failed' and \
                  dictresult['requestDetails'].has_key(u'RequestMessages'):
            for messageL in dictresult['requestDetails'][u'RequestMessages']:
                #messages are lists
                for message in messageL:
                    self.logger.info("   Server Messages:")
                    self.logger.info("   \t%s" % message)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-f", "--failure",
                                 dest = 'failure',
                                 action = "store_true",
                                 default = False,
                                 help = "Provide details about failed jobs")

        self.parser.add_option( "-i", "--site",
                                 dest = "site",
                                 action = "store_true",
                                 default = False,
                                 help = "Provide details about sites" )
