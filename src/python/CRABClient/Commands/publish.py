import json
import os
import pycurl

from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand, ConfigCommand
from CRABClient.ServerInteractions import HTTPRequests

class publish(SubCommand, ConfigCommand):
    """ Publish the output datasets in the task identified by
    -t/--task option
    """

    def __call__(self):

        ## check input options

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')
        if self.options.dbs is None and self.options.config is None:
            return CommandResult(1, 'ERROR: DBS URL must be specified on the command line or in the config file')

        if self.options.config:
            valid, configmsg = self.loadConfig(self.options.config, self.args)

        ## retrieving output files location from the server
        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Requesting publication for task %s' % self.cachedinfo['RequestName'] )

        if self.options.dbs:
            inputdict = {'PublishDbsUrl': self.options.dbs}
        else:
            valid, configmsg = self.validateConfig(checkValues = ['publishDBS'])
            if not valid:
                return CommandResult(1, configmsg)
            inputdict = {'PublishDbsUrl': self.configuration.Data.publishDbsUrl}

        try:
            dictresult, status, reason = server.post(self.uri + self.cachedinfo['RequestName'], json.dumps(inputdict, sort_keys=False))
        except pycurl.error as e:
            if e.args[0] == pycurl.E_OPERATION_TIMEOUTED:
                return CommandResult(1, 'Publication has been started. Re-issue publish command in a few minutes for results.')
            raise
        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem publishing the task to the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            return CommandResult(1, msg)

        success = dictresult['status']
        self.logger.info(dictresult['message'])
        summary = dictresult.get('summary', {})
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

        self.parser.add_option( "-c", "--config",
                                 dest = "config",
                                 default = None,
                                 help = "CRAB configuration file",
                                 metavar = "FILE" )

    def validateConfig(self, checkValues=None):
        """
        __validateConfig__

        Checking if needed input parameters are there
        """

        if checkValues:
            if not getattr(self.configuration, 'Data', None):
                return False, "Crab configuration problem: Data section is missing."
            else:
                if not hasattr(self.configuration.Data, 'publishDbsUrl'):
                    return False, "Neither command line nor config specifies a DBS URL."

        return True, "Valid configuration"
