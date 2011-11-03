from CRABClient.Commands import CommandResult
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
import os
import json

class resubmit(SubCommand):
    """ Resubmit the failed jobs of the task identified by
    -t/--task option
    """

    ## name should become automatically generated
    name  = "resubmit"
    names = [name]
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        ## check input options

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')

        ## retrieving output files location from the server
        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Requesting resubmission for failed jobs in task %s' % self.cachedinfo['RequestName'] )
        force = 0
        if self.options.force is True:
            force = 1
            self.logger.debug("Forcing resubmission")
        inputdict = { "TaskResubmit": "Analysis", "ForceResubmit" : force }
        dictresult, status, reason = server.post(self.uri + self.cachedinfo['RequestName'], json.dumps(inputdict, sort_keys = False))

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving resubmitting the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            return CommandResult(1, msg)

        self.logger.info("Resubmission succesfully requested")
        return CommandResult(0, '')


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Same as -c/-continue" )

        self.parser.add_option( '--force',
                                action = "store_true",
                                dest = 'force',
                                default = False,
                                help = 'Force the resubmission when the task is not yet complete' )
