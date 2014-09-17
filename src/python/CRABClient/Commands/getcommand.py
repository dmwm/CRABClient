from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import ConfigurationException , RESTCommunicationException
from CRABClient.client_utilities import validateJobids
from CRABClient import __version__
import CRABClient.Emulator

import os
import re
import copy

class getcommand(SubCommand):
    """ Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
        is identified by -t/--task option
    """

    visible = False


    def __call__(self, **argv):
        #Setting default destination if -o is not provided
        if not self.dest:
            self.dest = os.path.join(self.requestarea, 'results')

        # Destination is a URL.
        if re.match("^[a-z]+://", self.dest):
            if not self.dest.endswith("/"):
                self.dest += "/"
        #Creating the destination directory if necessary
        elif not os.path.exists( self.dest ):
            self.logger.debug("Creating directory %s " % self.dest)
            os.makedirs( self.dest)
        elif not os.path.isdir( self.dest ):
            raise ConfigurationException('Destination directory is a file')

        if self.options.dump or self.options.xroot:
            self.logger.debug("Getting url info")
        else:
            self.logger.info("Setting the destination to %s " % self.dest )

        #Retrieving output files location from the server
        self.logger.debug('Retrieving locations for task %s' % self.cachedinfo['RequestName'] )
        inputlist =  [ ('workflow', self.cachedinfo['RequestName']) ]
        inputlist.extend(list(argv.iteritems()))
        if getattr(self.options, 'quantity', None):
            self.logger.debug('Retrieving %s file locations' % self.options.quantity )
            inputlist.append( ('limit',self.options.quantity) )
        else:
            self.logger.debug('Retrieving all file locations')
            inputlist.append( ('limit', -1) )
        if getattr(self.options, 'jobids', None):
            self.logger.debug('Retrieving jobs %s' % self.options.jobids )
            inputlist.extend( self.options.jobids )
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, status, reason = server.get(self.uri, data = inputlist)
        self.logger.debug('Server result: %s' % dictresult )
        dictresult = self.processServerResult(dictresult)

        if status != 200:
            msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        totalfiles = len( dictresult['result'] )
        cpresults = []
#        for workflow in dictresult['result']: TODO re-enable this when we will have resubmissions
        workflow = dictresult['result']        #TODO assigning workflow to dictresult. for the moment we have only one wf
        arglist = ['--destination', self.dest, '--input', workflow, '-t', self.options.task, '--skip-proxy', self.proxyfilename, '--parallel',self.options.nparallel, '--wait',self.options.waittime]
        if len(workflow) > 0:
            if self.options.xroot:
                self.logger.debug("XRootD urls are requested")
                xrootlfn = ["root://cms-xrd-global.cern.ch/%s" % link['lfn'] for link in workflow]
                self.logger.info("\n".join(xrootlfn))
                returndict = {'xrootd' : xrootlfn}

            elif self.dump:
                pfnlist = map(lambda x: x['pfn'], workflow)
                self.logger.info("\n".join(pfnlist))
                returndict = {'pfn' :pfnlist}

            else:
                self.logger.info("Retrieving %s files" % totalfiles )
                copyoutput = remote_copy( self.logger, arglist )
                successdict, faileddict = copyoutput()

                #need to use deepcopy because successdict and faileddict are dict that is under the a manage dict, accessed multithreadly
                returndict = {'success' : copy.deepcopy(successdict) , 'failed' : copy.deepcopy(faileddict)}


        if totalfiles == 0:
            self.logger.info("No files to retrieve")
            returndict = {'success' : {} , 'failed' : {}}

        return returndict


    def processServerResult(self, result):
        #no modifications by default
        return result


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '--outputpath',
                                dest = 'outputpath',
                                default = None,
                                help = 'Where the files retrieved will be stored.  Defaults to the results/ directory.',
                                metavar = 'URL' )

        self.parser.add_option( '--dump',
                                dest = 'dump',
                                default = False,
                                action = 'store_true',
                                help = 'Instead of performing the transfer, dump the source URLs.' )

        self.parser.add_option( '--xrootd',
                                dest = 'xroot',
                                default = False,
                                action = 'store_true',
                                help = 'Give XrootD url for the file.')

        self.parser.add_option( '--jobids',
                                dest = 'jobids',
                                default = None,
                                help = 'Ids of the jobs you want to retrieve. Comma separated list of integers.',
                                metavar = 'JOBIDS' )


    def validateOptions(self):
        #Figuring out the destination directory
        SubCommand.validateOptions(self)
        self.dest = None
        if self.options.outputpath is not None:
            if re.match("^[a-z]+://", self.options.outputpath):
                self.dest=self.options.outputpath
            elif not os.path.isabs( self.options.outputpath ):
                self.dest = os.path.abspath( self.options.outputpath )
            else:
                self.dest = self.options.outputpath

        #convert all to -1
        if getattr(self.options, 'quantity', None) == 'all':
            self.options.quantity = -1

        #check the format of jobids
        if getattr(self.options, 'jobids', None):
            self.options.jobids = validateJobids(self.options.jobids)

        self.dump = self.options.dump
