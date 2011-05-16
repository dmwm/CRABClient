from Commands import CommandResult
from Commands.remote_copy import remote_copy
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import loadCache, getWorkArea
import json
import os


class getlog(SubCommand):

    ## name should become automatically generated
    name  = "getlog"
    usage = "usage: %prog " + name + " [options] [args]"

    def __call__(self, args):

        (options, args) = self.parser.parse_args( args )

        if options.task is None:
            return CommandResult(1, 'Error: Task option is required')
        if options.range is None:
            return CommandResult(1, 'Error: Range option is required')

        uri = '/crabinterface/crab/log'

        ## check input options and set destination directory

        requestarea, requestname = getWorkArea( options.task )
        cachedinfo = loadCache(requestarea, self.logger)


        dest = os.path.join(requestarea, 'results')
        if options.outputpath is not None:
            if os.path.isabs( options.outputpath ):
                dest = os.path.abspath( options.outputpath )
            else:
                dest = options.outputpath

        self.logger.debug("Setting the destination directory to %s " % dest )
        if not os.path.exists( dest ):
            self.logger.debug("Creating directory %s " % dest)
            os.makedirs( dest )
        elif not os.path.isdir( dest ):
            return CommandResult(1, 'Destination directory is a file')

        ## retrieving output files location from the server
        server = HTTPRequests(cachedinfo['Server'] + ':' + str(cachedinfo['Port']))

        self.logger.debug('Retrieving log files for jobs %s in task %s' % ( options.range, cachedinfo['RequestName'] ) )
        inputdict = {'jobRange' : options.range, 'requestID': cachedinfo['RequestName'] }
        dictresult, status, reason = server.get(uri, inputdict)

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving getoutput information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
            return CommandResult(1, msg)

        copyoutput = remote_copy( self.logger )
        return copyoutput(['-d', dest, '-i', dictresult, '-e', 'tgz'])


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

