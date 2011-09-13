import json

from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import loadCache


class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """

    name  = __name__.split('.').pop()
    names = [name]
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Looking up good lumis for task %s' % self.cachedinfo['RequestName'])
        result, status, reason = server.get(self.uri + self.cachedinfo['RequestName'])

        self.logger.debug("Result: %s" % result)

        if status != 200:
            msg = "Problem retrieving good lumis:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(result), str(reason))
            return CommandResult(1, msg)
        dictresult = json.loads(result)

        nLumis = 0
        for run in dictresult:
            for lumiPairs in dictresult[run]:
                nLumis += (1 + lumiPairs[1] - lumiPairs[0])
        self.logger.info("Sucessfully analyzed %s lumi(s) from %s run(s)" % (nLumis, len(dictresult)))

        with open(self.options.file, 'w') as jsonFile:
            json.dump(dictresult, jsonFile)
            jsonFile.write("\n")
            self.logger.info("Summary of processed lumi sections written to %s" % self.options.file)

        return CommandResult(0, None)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Task name to report on " )

        self.parser.add_option( "-o", "--outputfile",
                                 dest = "file",
                                 default = 'lumiReport.json',
                                 help = "Filename to write JSON summary to" )

