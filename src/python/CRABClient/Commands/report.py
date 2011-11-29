import json
import os

from FWCore.PythonUtilities.LumiList import LumiList

from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests


class report(SubCommand):
    """ Get the list of good lumis for your task identified by -t/--task option
    """

    name  = __name__.split('.').pop()
    names = [name]
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        if self.options.task is None:
            return CommandResult(2001, 'ERROR: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Looking up good lumis for task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri + self.cachedinfo['RequestName'])

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving good lumis:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        mergedLumis = LumiList()
        doubleLumis = LumiList()
        for workflow in dictresult[unicode("lumis")]:
            self.logger.info('#%s %s' % (workflow['subOrder'], workflow['request']) )
            nLumis = 0
            wflumi = json.loads(workflow[unicode("lumis")])
            doubleLumis = mergedLumis & LumiList(compactList = wflumi)
            mergedLumis = mergedLumis | LumiList(compactList = wflumi)
            for run in wflumi:
                for lumiPairs in wflumi[run]:
                    nLumis += (1 + lumiPairs[1] - lumiPairs[0])
            self.logger.info("   Sucessfully analyzed %s lumi(s) from %s run(s)" % (nLumis, len(wflumi)))
            if doubleLumis:
                self.logger.info("Warning: double run-lumis processed %s" % doubleLumis)

        if self.options.file:
            jsonFileName = self.options.file
        else:
            jsonFileName = os.path.join(self.requestarea, 'results', 'lumiReport.json')
        with open(jsonFileName, 'w') as jsonFile:
            json.dump(mergedLumis.getCompactList(), jsonFile)
            jsonFile.write("\n")
            self.logger.info("Summary of processed lumi sections written to %s" % jsonFileName)

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
                                 default = None,
                                 help = "Filename to write JSON summary to" )

