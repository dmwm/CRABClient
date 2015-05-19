from CRABClient.Commands.SubCommand import SubCommand
from CRABClient import __version__
from RESTInteractions import HTTPRequests
from datetime import datetime
from datetime import date, timedelta
import sys

class tasks(SubCommand):
    """ 
    Give back all the tasks starting from a specific date. If both fromdate and days are set use days
    """
    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, status, reason = server.get(self.uri, data = { 'timestamp' : self.date })
        dictresult = dictresult['result'] #take just the significant part
        dictresult.sort()
        dictresult.reverse()

        today = date.today() 

        self.logger.info ('\n')
        self.logger.info ('The list of tasks from %s until %s' %(self.date, today))
        self.logger.info ('='*80)
        self.logger.info ('NAME\t\t\t\t\t\t\t\tSTATUS')
        self.logger.info ('='*80)

        for item in dictresult:
            name, status = item[0:2]
            self.logger.info ('%s\n\t\t\t\t\t\t\t\t%s' %(name, status))
            self.logger.info ('-'*80)
    	
        self.logger.info ('\n')
	
    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
         
        self.parser.add_option( '--fromdate',
                                dest = 'fromdate',
                                default = '2000-01-01',
                                help = 'Give the user tasks fromdate=YYYY-MM-DD.',
                                metavar = 'YYYY-MM-DD' )

        self.parser.add_option( '--days',
                                dest = 'days',
                                default = 0,
                                help = 'Give the user tasks from the previous NUMBERS days.',
                                metavar = 'NUMBERS' )
        			    
    def validateOptions(self):
        try:
            datetime.strptime(self.options.fromdate, '%Y-%m-%d')
            if len(self.options.fromdate) == 10:
                self.date = self.options.fromdate
            else:
                self.logger.info ("\nPlease enter date with format 'YYYY-MM-DD'\nExample: crab tasks --fromdate=2014-01-01\n")
                sys.exit()
        except ValueError:
            raise ValueError("\nPlease enter date with format 'YYYY-MM-DD'\nExample: crab tasks --fromdate=2014-01-01")
        if self.options.days and self.options.fromdate:
            try:
                days = int(self.options.days)
                self.logger.info ('Task from previous %s days' % (days))
                self.date = date.today() - timedelta(days=days)
            except ValueError:
                raise ValueError("\nPlease enter a NUMBER\nExample: crab tasks --days=10")
