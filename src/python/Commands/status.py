from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import loadCache
import json


class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    ## name should become automatically generated
    name  = "status"
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )

        if options.task is None:
            return CommandResult(1, 'Error: Task option is required')

        uri = '/crabinterface/crab/task/'

        cachedinfo = loadCache(options.task, self.logger)

        server = HTTPRequests(cachedinfo['Server'] + ':' + str(cachedinfo['Port']))

        self.logger.debug('Looking up detailed status of task %s' % cachedinfo['RequestName'])
        dictresult, status, reason = server.get(uri + cachedinfo['RequestName'])

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)
 
        self.logger.info("Task Status:        %s"    % str(dictresult[unicode('RequestStatus')]))
        self.logger.info("Completed at level: %s%% " % str(dictresult['percent_success']))
    
        return CommandResult(0, None)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Same as -c/-continue" )

        pass

