from CRABClient.Commands.remote_copy import remote_copy
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ConfigurationException , RESTCommunicationException
from CRABClient.ClientUtilities import validateJobids, colors
from CRABClient import __version__
import CRABClient.Emulator

import os
import re
import copy
import urllib

class getcommand(SubCommand):
    """
    Retrieve the output files of a number of jobs specified by the -q/--quantity option. The task
    is identified by the -d/--dir option.
    """

    visible = False


    def __call__(self, **argv):
        ## Retrieve the transferLogs parameter from the task database.
        taskdbparam, configparam = '', ''
        if argv.get('subresource') == 'logs':
            taskdbparam = 'tm_save_logs'
            configparam = "General.transferLogs"
        elif argv.get('subresource') == 'data':
            taskdbparam = 'tm_transfer_outputs'
            configparam = "General.transferOutputs"

        transferFlag = 'unknown'
        inputlist = {'subresource': 'search', 'workflow': self.cachedinfo['RequestName']}
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        uri = self.getUrl(self.instance, resource = 'task')
        dictresult, status, reason =  server.get(uri, data = inputlist)
        self.logger.debug('Server result: %s' % dictresult)
        if status == 200:
            if 'desc' in dictresult and 'columns' in dictresult['desc']:
                position = dictresult['desc']['columns'].index(taskdbparam)    
                transferFlag = dictresult['result'][position] #= 'T' or 'F'
            else:
                self.logger.debug("Unable to locate %s in server result." % (taskdbparam))
        ## If transferFlag = False, there is nothing to retrieve.
        if transferFlag == 'F':
            msg = "No files to retrieve. Files not transferred to storage since task configuration parameter %s is False." % (configparam) 
            self.logger.info(msg)
            return {'success': {}, 'failed': {}}

        ## Retrieve tm_edm_outfiles, tm_tfile_outfiles and tm_outfiles from the task database and check if they are empty.
        if argv.get('subresource') == 'data' and status == 200:
            if 'desc' in dictresult and 'columns' in dictresult['desc']:
                position = dictresult['desc']['columns'].index('tm_edm_outfiles')
                tm_edm_outfiles = dictresult['result'][position]
                position = dictresult['desc']['columns'].index('tm_tfile_outfiles')
                tm_tfile_outfiles = dictresult['result'][position]
                position = dictresult['desc']['columns'].index('tm_outfiles')
                tm_outfiles = dictresult['result'][position]
            if tm_edm_outfiles == '[]' and tm_tfile_outfiles == '[]' and tm_outfiles == '[]':
                msg  = "%sWarning%s:" % (colors.RED, colors.NORMAL)
                msg += " There are no output files to retrieve, because CRAB could not detect any in the CMSSW configuration"
                msg += " nor was any explicitly specified in the CRAB configuration."
                self.logger.warning(msg)

        #Retrieving output files location from the server
        self.logger.debug('Retrieving locations for task %s' % self.cachedinfo['RequestName'])
        inputlist =  [('workflow', self.cachedinfo['RequestName'])]
        inputlist.extend(list(argv.iteritems()))
        if getattr(self.options, 'quantity', None):
            self.logger.debug('Retrieving %s file locations' % self.options.quantity)
            inputlist.append(('limit', self.options.quantity))
        else:
            self.logger.debug('Retrieving all file locations')
            inputlist.append(('limit', -1))
        if getattr(self.options, 'jobids', None):
            self.logger.debug('Retrieving jobs %s' % self.options.jobids)
            inputlist.extend(self.options.jobids)
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        dictresult, status, reason = server.get(self.uri, data = urllib.urlencode(inputlist))
        self.logger.debug('Server result: %s' % dictresult)

        if status != 200:
            msg = "Problem retrieving information from the server:\ninput:%s\noutput:%s\nreason:%s" % (str(inputlist), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        totalfiles = len(dictresult['result'])
        cpresults = []
        workflow = dictresult['result']
        if len(workflow) > 0:
            if self.options.dump or self.options.xroot:
                self.logger.debug("Getting url info")
            else:
                self.setDestination()
                self.logger.info("Setting the destination to %s " % self.dest)
            if self.options.xroot:
                self.logger.debug("XRootD urls are requested")
                xrootlfn = ["root://cms-xrd-global.cern.ch/%s" % link['lfn'] for link in workflow]
                self.logger.info("\n".join(xrootlfn))
                returndict = {'xrootd': xrootlfn}
            elif self.options.dump:
                jobid_pfn_lfn_list = map(lambda x: (x['jobid'], x['pfn'], x['lfn']), workflow)
                jobid_pfn_lfn_list.sort()
                lastjobid = -1
                filecounter = 1
                msg = ""
                for jobid, pfn, lfn in jobid_pfn_lfn_list:
                    if jobid != lastjobid:
                        msg += "%s=== Files from job %s:" % ('\n' if lastjobid != -1 else '', jobid)
                        lastjobid = jobid
                        filecounter = 1
                    msg += "\n%d) PFN: %s" % (filecounter, pfn)
                    msg += "\n%s  LFN: %s" % (' '*(len(str(filecounter))), lfn)
                    filecounter += 1
                self.logger.info(msg)
                returndict = {'pfn': [pfn for _, pfn, _ in jobid_pfn_lfn_list], 'lfn': [lfn for _, _, lfn in jobid_pfn_lfn_list]}
            else:
                self.logger.info("Retrieving %s files" % (totalfiles))
                arglist = ['--destination', self.dest, '--input', workflow, '--dir', self.options.projdir, \
                           '--proxy', self.proxyfilename, '--parallel', self.options.nparallel, '--wait', self.options.waittime, \
                           '--checksum', self.checksum, '--command', self.command]
                copyoutput = remote_copy(self.logger, arglist)
                successdict, faileddict = copyoutput()
                #need to use deepcopy because successdict and faileddict are dict that is under the a manage dict, accessed multithreadly
                returndict = {'success': copy.deepcopy(successdict) , 'failed': copy.deepcopy(faileddict)}
        if totalfiles == 0:
            self.logger.info("No files to retrieve.")
            returndict = {'success': {} , 'failed': {}}

        if transferFlag == 'unknown':
            if ('success' in returndict and not returndict['success']) and \
               ('failed'  in returndict and not returndict['failed']):
                msg = "This is normal behavior if %s = False in the task configuration." % (configparam)
                self.logger.info(msg)

        return returndict


    def setDestination(self):
        #Setting default destination if -o is not provided
        if not self.dest:
            self.dest = os.path.join(self.requestarea, 'results')
        # Destination is a URL.
        if re.match("^[a-z]+://", self.dest):
            if not self.dest.endswith("/"):
                self.dest += "/"
        #Creating the destination directory if necessary
        elif not os.path.exists(self.dest):
            self.logger.debug("Creating directory %s " % self.dest)
            os.makedirs(self.dest)
        elif not os.path.isdir(self.dest):
            raise ConfigurationException('Destination directory is a file')


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
        self.parser.add_option('--checksum',
                                dest = 'checksum',
                                default = 'yes',
                                help = 'Set it to true if needed. If true will use ADLER32 checksum' +\
                                       'Allowed values are yes/no. Default is yes.')
        self.parser.add_option('--command',
                                dest = 'command',
                                default = None,
                                help = 'A command which to use. Available commands are LCG or GFAL.')

    def validateOptions(self):
        #Figuring out the destination directory
        SubCommand.validateOptions(self)
        self.dest = None
        if self.options.outputpath is not None:
            if re.match("^[a-z]+://", self.options.outputpath):
                self.dest = self.options.outputpath
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

        if hasattr(self.options, 'command') and self.options.command != None:
            AvailableCommands = ['LCG', 'GFAL']
            self.command = self.options.command.upper()
            if self.command not in AvailableCommands:
                msg = "You specified to use %s command and it is not allowed. Available commands are: %s " % (self.command, str(AvailableCommands))
                ex = ConfigurationException(msg)
                raise ex
        else:
            self.command = None
        if hasattr(self.options, 'checksum'):
            if re.match('^yes$|^no$', self.options.checksum):
                self.checksum = 'ADLER32' if self.options.checksum == 'yes' else None
            else:
                msg = "You specified to use %s checksum. Only lowercase yes/no is accepted to turn ADLER32 checksum" % self.options.checksum
                ex = ConfigurationException(msg)
                raise ex
