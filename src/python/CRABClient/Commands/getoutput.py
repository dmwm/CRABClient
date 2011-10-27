from CRABClient.Commands import CommandResult
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
import os

class getoutput(SubCommand):
    """ Retrieve the output files of the jobs specified by -r/--range option part of
    the task identified by -t/--task option
    """

    ## name should become automatically generated
    name  = "get-output"
    names = [name, 'output']
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self):

        ## check input options and set destination directory

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')
        if self.options.range is None:
            return CommandResult(1, 'ERROR: Range option is required')

        dest = os.path.join(self.requestarea, 'results')
        if self.options.outputpath is not None:
            if not os.path.isabs( self.options.outputpath ):
                dest = os.path.abspath( self.options.outputpath )
            else:
                dest = self.options.outputpath

        self.logger.debug("Setting the destination directory to %s " % dest )
        if not os.path.exists( dest ):
            self.logger.debug("Creating directory %s " % dest)
            os.makedirs( dest )
        elif not os.path.isdir( dest ):
            return CommandResult(1, 'Destination directory is a file')

        ## retrieving output files location from the server
        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Retrieving output for jobs %s in task %s' % ( self.options.range, self.cachedinfo['RequestName'] ) )
        inputdict = {'jobRange' : self.options.range, 'requestID': self.cachedinfo['RequestName'] }
        dictresult, status, reason = server.get(self.uri, inputdict)

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving getoutput information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            return CommandResult(1, msg)

        arglist = ['-d', dest, '-i', dictresult]
        if self.options.skipProxy:
            arglist.append('-p')
        copyoutput = remote_copy( self.logger, arglist )
        return copyoutput()


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Same as -c/-continue" )

        self.parser.add_option( '-r', '--range',
                                dest = 'range',
                                default = None,
                                help = 'List or range to work with' )

        self.parser.add_option( '-o', '--outputpath',
                                dest = 'outputpath',
                                default = None,
                                help = 'Where the output files retrieved will be stored in the local file system',
                                metavar = 'DIRECTORY' )

        self.parser.add_option( "-p", "--skip-proxy",
                                action = "store_true",
                                dest = "skipProxy",
                                default = None,
                                help = "Skip Grid proxy creation and myproxy delegation")

