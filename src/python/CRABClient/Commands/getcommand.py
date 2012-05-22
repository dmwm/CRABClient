from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigurationException
import os

class getcommand(SubCommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by -t/--task option
    """

    visible = False

    def __call__(self, subresource):
        #Creating the destination directory if necessary
        self.logger.info("Setting the destination directory to %s " % self.dest )

        self.dest = os.path.join(self.requestarea, 'results')
        #Creating the destination directory if necessary
        if not os.path.exists( self.dest ):
            self.logger.debug("Creating directory %s " % self.dest)
            os.makedirs( self.dest )
        elif not os.path.isdir( self.dest ):
            raise ConfigurationException('Destination directory is a file')

        #Retrieving output files location from the server
        self.logger.debug('Retrieving file location for %s of task %s' % ( self.options.quantity, self.cachedinfo['RequestName'] ) )
        server = HTTPRequests(self.serverurl, self.proxyfilename)
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName'], 'subresource' : subresource, 'limit' : self.options.quantity })
        self.logger.debug('Server result: %s' % dictresult )

        if status != 200:
            msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputdict), str(dictresult), str(reason))
            raise ConfigurationException(msg)

        totalfiles = len( dictresult['result'] )
        cpresults = []
#        for workflow in dictresult['result']: TODO re-enable this when we will have resubmissions
        workflow = dictresult['result']        #TODO assigning workflow to dictresult. for the moment we have only one wf
        arglist = ['-d', self.dest, '-i', workflow]
        if self.options.skipProxy:
            arglist.append('-p')
        if len(workflow) > 0:
            self.logger.info("Retrieving %s files" % totalfiles )
            copyoutput = remote_copy( self.logger, arglist )
            copyoutput()

        if totalfiles == 0:
            self.logger.info("No file to retrieve")


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-o', '--outputpath',
                                dest = 'outputpath',
                                default = None,
                                help = 'Where the files retrieved will be stored in the local file system',
                                metavar = 'DIRECTORY' )

    def validateOptions(self):
        #Figuring out the destination directory
        SubCommand.validateOptions(self)
        self.dest = None
        if self.options.outputpath is not None:
            if not os.path.isabs( self.options.outputpath ):
                self.dest = os.path.abspath( self.options.outputpath )
            else:
                self.dest = self.options.outputpath
