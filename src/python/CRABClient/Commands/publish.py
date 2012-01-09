from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
import os
import json

class publish(SubCommand):
    """ Publish the output datasets in the task identified by
    -t/--task option
    """

    name  = __name__.split('.').pop()
    names = [name]
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        ## check input options

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')
        if self.options.dbs is None:
            return CommandResult(1, 'ERROR: DBS URL option is required')

        ## retrieving output files location from the server
        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Requesting publication for task %s' % self.cachedinfo['RequestName'] )

        inputdict = {'PublishDbsUrl': self.options.dbs}
        dictresult, status, reason = server.post(self.uri + self.cachedinfo['RequestName'], json.dumps(inputdict, sort_keys=False))
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem publishing the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            return CommandResult(1, msg)

        success = dictresult['status']
        self.logger.info(dictresult['message'])
        summary = dictresult['summary']
        for datasetName in summary:
            if summary[datasetName]['existingFiles']:
                self.logger.info(' Dataset %s, %s files already exist, added %s new files in %s new blocks' %
                                 (datasetName, summary[datasetName]['existingFiles'],
                                  summary[datasetName]['files'], summary[datasetName]['blocks']))
            else:
                self.logger.info(' Dataset %s created with %s files in %s new blocks' %
                                 (datasetName, summary[datasetName]['files'], summary[datasetName]['blocks']))

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

        self.parser.add_option( "-u", "--dbs",
                                 dest = "dbs",
                                 default = None,
                                 help = "DBS server URL" )
