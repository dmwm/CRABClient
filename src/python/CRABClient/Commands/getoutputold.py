from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ClientExceptions import ConfigurationException
import os

class getoutputold(getcommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by the -d/--dir option
    """
    name = 'getoutputold'
    shortnames = ['outputold', 'outold']
    visible = True #overwrite getcommand

    def __call__(self):
        returndict = getcommand.__call__(self, subresource = 'data')

        return returndict

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--quantity',
                                dest = 'quantity',
                                help = 'The number of output files you want to retrieve (or "all"). Ignored if --jobids is used.' )
        self.parser.add_option( '--parallel',
                                dest = 'nparallel',
                                help = 'Number of parallel download, default is 10 parallel download.',)
        self.parser.add_option( '--wait',
                                dest = 'waittime',
                                help = 'Increase the sendreceive-timeout in second',)
        getcommand.setOptions(self)
