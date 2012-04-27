from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
import os


class getlog(SubCommand):
    """ Retrieve the log files of the jobs specified by -r/--range option part of the
    task identified by -t/--task option
    """
    #TODO delete CommandResult and use raise Exception
    #TODO use requiresTaskOption in ClientMapping and validateOptions
    visible = False

    shortnames = ['log']

    def __call__(self):

        ## check input options and set destination directory

        if self.options.task is None:
            return CommandResult(2001, 'ERROR: Task option is required')
        if self.options.range is None:
            return CommandResult(2002, 'ERROR: Range option is required')

        dest = os.path.join(self.requestarea, 'results')
        if self.options.outputpath is not None:
            if not os.path.isabs( self.options.outputpath ):
                dest = os.path.abspath( self.options.outputpath )
            else:
                dest = self.options.outputpath

        self.logger.info("Setting the destination directory to %s " % dest )
        if not os.path.exists( dest ):
            self.logger.debug("Creating directory %s " % dest)
            os.makedirs( dest )
        elif not os.path.isdir( dest ):
            return CommandResult(1, 'Destination directory is a file')

        ## retrieving output files location from the server
        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Retrieving log file location for jobs %s in task %s' % ( self.options.range, self.cachedinfo['RequestName'] ) )
        inputdict = {'jobRange' : self.options.range}
        dictresult, status, reason = server.get(self.uri + self.cachedinfo['RequestName'], inputdict)

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving getoutput information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            return CommandResult(1, msg)

        totalfiles = 0
        cpresults = []
        for workflow in dictresult['log']:
            arglist = ['-d', dest, '-i', workflow['output'], '-e' , 'tgz']
            if self.options.skipProxy:
                arglist.append('-p')
            if len(workflow['output']) > 0:
                totalfiles += len(workflow['output'])
                self.logger.info("Found %i log files for requested range" % len(workflow['output']) )
                copyoutput = remote_copy( self.logger, arglist )
                cpresults.append( copyoutput() )
            else:
                cpresults.append( CommandResult(0, '') )
        if totalfiles == 0:
            self.logger.info("No log file to retrieve for requested range")
        return mergeResults( cpresults )


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
#moved to SubOption
#        self.parser.add_option( "-p", "--skip-proxy",
#                                action = "store_true",
#                                dest = "skipProxy",
#                                default = None,
#                                help = "Skip Grid proxy creation and myproxy delegation")

