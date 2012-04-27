from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigurationException
import os

class getoutput(SubCommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by -t/--task option
    """

    ## name should become automatically generated
    shortnames = ['output', 'out']

    def __call__(self):
        #Figuring out the destination directory
        dest = os.path.join(self.requestarea, 'results')
        if self.options.outputpath is not None:
            if not os.path.isabs( self.options.outputpath ):
                dest = os.path.abspath( self.options.outputpath )
            else:
                dest = self.options.outputpath

        #Creating the destination directory if necessary
        self.logger.info("Setting the destination directory to %s " % dest )

        #Retrieving output files location from the server
        self.logger.debug('Retrieving output file location for %s of task %s' % ( self.options.quantity, self.cachedinfo['RequestName'] ) )
        server = HTTPRequests(self.serverurl, self.proxyfilename)
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName'], 'subresource' : 'data', 'limit' : self.options.quantity })
        self.logger.debug('Getoutput server result: %s' % dictresult )

        if status != 200:
            msg = "Problem retrieving getoutput information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            raise ConfigurationException(msg)

        totalfiles = len( dictresult['result'] )
        cpresults = []
#        for workflow in dictresult['result']: TODO re-enable this when we will have resubmissions
        workflow = dictresult['result']        #TODO assigning workflow to dictresult. for the moment we have only one wf
        arglist = ['-d', dest, '-i', workflow]
        if self.options.skipProxy:
            arglist.append('-p')
        if len(workflow) > 0:
            self.logger.info("Retrieving %s files" % totalfiles )
            copyoutput = remote_copy( self.logger, arglist )

        if totalfiles == 0:
            self.logger.info("No output file to retrieve")


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-q', '--quantity',
                                dest = 'quantity',
                                default = -1,
                                help = 'A number which express the number of files you want to retrieve. Defaut retrieve all outputs' )

        self.parser.add_option( '-o', '--outputpath',
                                dest = 'outputpath',
                                default = None,
                                help = 'Where the output files retrieved will be stored in the local file system',
                                metavar = 'DIRECTORY' )

    def validateOptions(self):
        #Creating the destination directory if necessary
        if not os.path.exists( dest ):
            self.logger.debug("Creating directory %s " % dest)
            os.makedirs( dest )
        elif not os.path.isdir( dest ):
            raise ConfigurationException('Destination directory is a file')
