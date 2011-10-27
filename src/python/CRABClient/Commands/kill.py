from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests


class kill(SubCommand):
    """
    Simply call the server side of the kill
    """

    name  = __name__.split('.').pop()
    names = [name, 'kill']
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self):
        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Killing task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.delete(self.uri + self.cachedinfo['RequestName'])
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem killing task %s:\ninput:%s\noutput:%s\nreason:%s" % \
                    (self.cachedinfo['RequestName'], str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        self.logger.info("Task killed")

        return CommandResult(0, None)

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Kill a task" )
