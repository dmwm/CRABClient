from datetime import datetime, date, timedelta

from RESTInteractions import HTTPRequests
from ServerUtilities import TASKDBSTATUSES

from CRABClient import __version__
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ConfigurationException, RESTCommunicationException


class tasks(SubCommand):
    """Give back all user tasks starting from a specific date. Default is last 30 days.
Note that STATUS in here is task status from database which does not include grid jobs,
task status does not progress beyond SUBMITTED unless the task is KILLED
    """
    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, status, reason = server.get(self.uri, data = {'timestamp': self.date})
        dictresult = dictresult['result'] #take just the significant part

        if status != 200:
            msg = "Problem retrieving tasks:\ninput:%s\noutput:%s\nreason:%s" % (str(self.date), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        dictresult.sort()
        dictresult.reverse()

        if self.options.status:
            dictresult = [item for item in dictresult if item[1] == self.options.status]

        result = [item[0:2] for item in dictresult]

        today = date.today()

        if not dictresult:
            msg = "No tasks found from %s until %s" % (self.date, today)
            if self.options.status:
                msg += " with status %s" % (self.options.status)
            self.logger.info(msg)
            return result

        msg = "\nList of tasks from %s until %s" % (self.date, today)
        if self.options.status:
            msg += " with status %s" % (self.options.status)
        self.logger.info(msg)
        msg = "Beware that STATUS here does not include information from grid jobs"
        self.logger.info(msg)
        self.logger.info('='*80)
        self.logger.info('NAME\t\t\t\t\t\t\t\tSTATUS')
        self.logger.info('='*80)
        for item in dictresult:
            name, status = item[0:2]
            self.logger.info('%s\n\t\t\t\t\t\t\t\t%s' % (name, status))
            self.logger.info('-'*80)
        self.logger.info('\n')

        return result


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( '--fromdate',
                                dest = 'fromdate',
                                default = None,
                                help = 'Give the user tasks since YYYY-MM-DD.',
                                metavar = 'YYYY-MM-DD' )

        self.parser.add_option( '--days',
                                dest = 'days',
                                default = None,
                                type = 'int',
                                help = 'Give the user tasks from the previous N days.',
                                metavar = 'N' )

        self.parser.add_option( '--status',
                                dest = 'status',
                                default = None,
                                help = 'Give the user tasks with the given STATUS.',
                                metavar = 'STATUS' )

        			    
    def validateOptions(self):

        if self.options.fromdate is not None and self.options.days is not None:
            msg = "Options --fromdate and --days cannot be used together. Please specify only one of them."
            raise ConfigurationException(msg)
        if self.options.fromdate is None and self.options.days is None:
            days = 30
            self.date = date.today() - timedelta(days=days)
        elif self.options.days is not None:
            days = self.options.days
            self.date = date.today() - timedelta(days=days)
        elif self.options.fromdate is not None:
            try:
                datetime.strptime(self.options.fromdate, '%Y-%m-%d')
            except ValueError:
                msg = "Please enter date with format 'YYYY-MM-DD'. Example: crab tasks --fromdate=2014-01-01"
                raise ConfigurationException(msg)
            if len(self.options.fromdate) != 10:
                msg = "Please enter date with format 'YYYY-MM-DD'. Example: crab tasks --fromdate=2014-01-01"
                raise ConfigurationException(msg)
            self.date = self.options.fromdate

        if self.options.status is not None:
            if self.options.status not in TASKDBSTATUSES:
                msg = "Please enter a valid task status. Valid task statuses are: %s" % (TASKDBSTATUSES)
                raise ConfigurationException(msg) 
