from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientExceptions import ConfigurationException

import os

class getlog(getcommand):
    """
    Retrieve the log files of a number of jobs specified by the -q/--quantity option.
    -q logfiles per exit code are returned if transferLogs = False; otherwise all the log files
    collected by the LogCollect job are returned. The task is identified by the -d/--dir option.
    """
    name = 'getlog'
    shortnames = ['log']
    visible = True #overwrite getcommand

    def __call__(self):
        returndict = getcommand.__call__(self, subresource = 'logs')
        if not returndict.get('success', {}) and not returndict.get('failed', {}):
            msg = "This is normal behavior unless General.transferLogs=True is present in the task configuration."
            self.logger.info(msg)
        return returndict

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--quantity',
                                dest = 'quantity',
                                help = 'The number of logs you want to retrieve (or "all"). Ignored if --jobids is used.' )
        self.parser.add_option( '--parallel',
                                dest = 'nparallel',
                                help = 'Number of parallel download, default is 10 parallel download.',)
        self.parser.add_option( '--wait',
                                dest = 'waittime',
                                help = 'Increase the sendreceive-timeout in second.',)
        getcommand.setOptions(self)

