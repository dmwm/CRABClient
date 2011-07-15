import json

from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import loadCache


class report(SubCommand):
    """
    Get the list of good lumis for your task identified by -t/--task option
    """

    name  = __name__.split('.').pop()
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self, args):
        (options, args) = self.parser.parse_args(args)

        if options.task is None:
            return CommandResult(1, 'Error: Task option is required')

        cachedinfo = loadCache(options.task, self.logger)

        server = HTTPRequests(cachedinfo['Server'] + ':' + str(cachedinfo['Port']))

        self.logger.debug('Looking up good lumis for task %s' % cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri + cachedinfo['RequestName'])

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving good lumis:\ninput:%s\noutput:%s\nreason:%s" % (str(cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        nLumis = 0
        for run in dictresult:
            for lumiPairs in dictresult[run]:
                nLumis += (1 + lumiPairs[1] - lumiPairs[0])
        self.logger.info("Sucessfully analyzed %s lumi(s) from %s run(s)" % (nLumis, len(dictresult)))

        with open(options.file, 'w') as jsonFile:
            json.dump(dictresult, jsonFile)
            jsonFile.write("\n")
            self.logger.info("Summary of processed lumi sections written to %s" % options.file)

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

