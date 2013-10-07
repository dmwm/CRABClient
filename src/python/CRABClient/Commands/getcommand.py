from CRABClient.Commands import CommandResult, mergeResults
from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests
from CRABClient.client_exceptions import ConfigurationException
from CRABClient.client_utilities import validateJobids

import os
import re

class getcommand(SubCommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by -t/--task option
    """

    visible = False

    def __call__(self, **argv):
        #Setting default destination if -o is not provided
        if not self.dest:
            self.dest = os.path.join(self.requestarea, 'results')

        #Creating the destination directory if necessary
        if not os.path.exists( self.dest ):
            self.logger.debug("Creating directory %s " % self.dest)
            os.makedirs( self.dest )
        elif not os.path.isdir( self.dest ):
            raise ConfigurationException('Destination directory is a file')

        self.logger.info("Setting the destination directory to %s " % self.dest )
        print self.cachedinfo['OriginalConfig'].General
        self.standalone = getattr(self.cachedinfo['OriginalConfig'].General, 'standalone', False)
        print "Standalone is %s" % self.standalone
        if not self.standalone:
            #Retrieving output files location from the server
            self.logger.debug('Retrieving locations for task %s' % self.cachedinfo['RequestName'] )
            inputlist =  [ ('workflow', self.cachedinfo['RequestName']) ]
            inputlist.extend(list(argv.iteritems()))
            if getattr(self.options, 'quantity', None):
                self.logger.debug('Retrieving %s file locations' % self.options.quantity )
                inputlist.append( ('limit',self.options.quantity) )
            if getattr(self.options, 'jobids', None):
                self.logger.debug('Retrieving jobs %s' % self.options.jobids )
            inputlist.extend( self.options.jobids )
            server = HTTPRequests(self.serverurl, self.proxyfilename)
            dictresult, status, reason = server.get(self.uri, data = inputlist)
            self.logger.debug('Server result: %s' % dictresult )
            dictresult = self.processServerResult(dictresult)

            if status != 200:
                msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
                raise ConfigurationException(msg)

            workflow = dictresult['result']        #TODO assigning workflow to dictresult. for the moment we have only one wf
        else:
            dag = __import__("CRABInterface.DagmanDataWorkflow").DagmanDataWorkflow.DagmanDataWorkflow()
            quantity = getattr(self.options, 'quantity', -1)
            workflow = dag.outputLocation(self.cachedinfo['RequestName'], self.options.quantity, [])['result']
            inputlist =  [ ('workflow', self.cachedinfo['RequestName']) ]
            inputlist.extend(list(argv.iteritems()))
            if getattr(self.options, 'quantity', None):
                self.logger.debug('Retrieving %s file locations' % self.options.quantity )
                inputlist.append( ('limit',self.options.quantity) )
            if getattr(self.options, 'jobids', None):
                self.logger.debug('Retrieving jobs %s' % self.options.jobids )
                inputlist.extend( self.options.jobids )
            #server = HTTPRequests(self.serverurl, self.proxyfilename)
            #dictresult, status, reason = server.get(self.uri, data = inputlist)
            #raise RuntimeError, "Wtf is this"
            #dag.getFiles
            
        totalfiles = len( workflow )
        cpresults = []
#        for workflow in dictresult['result']: TODO re-enable this when we will have resubmissions
        workflow = dictresult['result']        #TODO assigning workflow to dictresult. for the moment we have only one wf
        arglist = ['-d', self.dest, '-i', workflow, '-t', self.options.task, '-p', self.proxyfilename]
        if len(workflow) > 0:
            self.logger.info("Retrieving %s files" % totalfiles )
            copyoutput = remote_copy( self.logger, arglist )
            copyoutput()
        if totalfiles == 0:
            self.logger.info("No files to retrieve")

    def processServerResult(self, result):
        #no modifications by default
        return result


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

        self.parser.add_option( '-i', '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to retrieve. Comma separated list of intgers',
                                metavar = 'JOBIDS' )

    def validateOptions(self):
        #Figuring out the destination directory
        SubCommand.validateOptions(self)
        self.dest = None
        if self.options.outputpath is not None:
            if not os.path.isabs( self.options.outputpath ):
                self.dest = os.path.abspath( self.options.outputpath )
            else:
                self.dest = self.options.outputpath

        #convert all to -1
        if getattr(self.options, 'quantity', None) == 'all':
            self.options.quantity = -1

        #check the format of jobids
        if getattr(self.options, 'jobids', None):
            self.options.jobids = validateJobids(self.options.jobids)
