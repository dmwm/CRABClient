from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.Commands.getcommand import getcommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigurationException
import os

class getoutput(getcommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by -t/--task option
    """
    name = 'getoutput'
    shortnames = ['output', 'out']
    visible = True #overwrite getcommand

    def __call__(self):
        getcommand.__call__(self, subresource = 'data')

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-q', '--quantity',
                                dest = 'quantity',
                                default = -1,
                                help = 'A number which express the number of files you want to retrieve. Defaut all outputs' )
        getcommand.setOptions(self)
