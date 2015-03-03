"""
This is simply taking care of job submission
"""
import json, os
from string import upper
import types
import imp
import urllib
import time

import CRABClient.Emulator

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import MissingOptionException, ConfigurationException, RESTCommunicationException
from CRABClient.ClientUtilities import getJobTypes, createCache, addPlugin, server_info, colors, getUrl
from CRABClient.ClientMapping import parametersMapping, getParamDefaultValue
from CRABClient import __version__


DBSURLS = {'reader': {'global': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                      'phys01': 'https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader',
                      'phys02': 'https://cmsweb.cern.ch/dbs/prod/phys02/DBSReader',
                      'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'},
           'writer': {'phys03': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'}}


class submit(SubCommand):
    """
    Perform the submission to the CRABServer
    """

    shortnames = ['sub']


    def __init__(self, logger, cmdargs = None):
        SubCommand.__init__(self, logger, cmdargs, disable_interspersed_args = True)


    def __call__(self):
        valid = False
        configmsg = 'Default'

        self.logger.debug("Started submission")
        serverFactory = CRABClient.Emulator.getEmulator('rest')

        uniquerequestname = None

        self.logger.debug("Working on %s" % str(self.requestarea))

        configreq = {}
        for param in parametersMapping['on-server']:
            mustbetype = getattr(types, parametersMapping['on-server'][param]['type'])
            default = parametersMapping['on-server'][param]['default']
            config_params = parametersMapping['on-server'][param]['config']
            for config_param in config_params:
                attrs = config_param.split('.')
                temp = self.configuration
                for attr in attrs:
                    temp = getattr(temp, attr, None)
                    if temp is None:
                        break
                if temp is not None:
                    configreq[param] = temp
                    break
                elif default is not None:
                    configreq[param] = default
                    temp = default
                else:
                    ## Parameter not strictly required.
                    pass
            ## Check that the requestname is of the right type.
            ## This is not checked in SubCommand.validateConfig().
            if param == 'workflow':
                if mustbetype == type(self.requestname):
                    configreq['workflow'] = self.requestname
            ## Translate boolean flags into integers.
            elif param in ['savelogsflag', 'publication', 'nonprodsw', 'useparent', 'ignorelocality', 'saveoutput', 'oneEventMode']:
                configreq[param] = 1 if temp else 0
            ## Translate DBS URL aliases into DBS URLs.
            elif param in ['dbsurl', 'publishdbsurl']:
                if param == 'dbsurl':
                    dbstype = 'reader'
                elif param == 'publishdbsurl':
                    dbstype = 'writer'
                allowed_dbsurls = DBSURLS[dbstype].values()
                allowed_dbsurls_aliases = DBSURLS[dbstype].keys()
                if configreq[param] in allowed_dbsurls_aliases:
                    configreq[param] = DBSURLS[dbstype][configreq[param]]
                elif configreq[param].rstrip('/') in allowed_dbsurls:
                    configreq[param] = configreq[param].rstrip('/')
            elif param == 'scriptexe' and 'scriptexe' in configreq:
                configreq[param] = os.path.basename(configreq[param])

        jobconfig = {}
        #get the backend URLs from the server external configuration
        serverBackendURLs = server_info('backendurls', self.serverurl, self.proxyfilename, getUrl(self.instance, resource='info'))
        #if cacheSSL is specified in the server external configuration we will use it to upload the sandbox (baseURL will be ignored)
        filecacheurl = serverBackendURLs['cacheSSL'] if 'cacheSSL' in serverBackendURLs else None
        pluginParams = [ self.configuration, self.logger, os.path.join(self.requestarea, 'inputs') ]
        crab_job_types = getJobTypes()
        if upper(configreq['jobtype']) in crab_job_types:
            plugjobtype = crab_job_types[upper(configreq['jobtype'])](*pluginParams)
            inputfiles, jobconfig, isbchecksum = plugjobtype.run(filecacheurl)
        else:
            fullname = configreq['jobtype']
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            inputfiles, jobconfig, isbchecksum = pluginInst.run()

        if configreq['publication']:
            non_edm_files = jobconfig['tfileoutfiles'] + jobconfig['addoutputfiles']
            if non_edm_files:
                msg = "%sWarning%s: The following output files will not be published, as they are not EDM files: %s" % (colors.RED, colors.NORMAL, non_edm_files)
                self.logger.warning(msg)

        if not configreq['publishname']:
            configreq['publishname'] =  isbchecksum
        else:
            configreq['publishname'] = "%s-%s" %(configreq['publishname'], isbchecksum)
        configreq.update(jobconfig)
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str(configreq))
        ## TODO: this shouldn't be hard-coded.
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist', \
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis', 'userfiles', 'scriptargs', 'extrajdl']
        configreq_encoded = self._encodeRequest(configreq, listParams)
        self.logger.debug('Encoded submit request: %s' % (configreq_encoded))

        dictresult, status, reason = server.put( self.uri, data = configreq_encoded)
        self.logger.debug("Result: %s" % dictresult)
        if status != 200:
            msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        elif dictresult.has_key("result"):
            uniquerequestname = dictresult["result"][0]["RequestName"]
        else:
            msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
                   % (str(configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        tmpsplit = self.serverurl.split(':')
        createCache(self.requestarea, tmpsplit[0], tmpsplit[1] if len(tmpsplit)>1 else '', uniquerequestname,
                    voRole=self.voRole, voGroup=self.voGroup, instance=self.instance,
                    originalConfig = self.configuration)

        self.logger.info("%sSuccess%s: Your task has been delivered to the CRAB3 server." %(colors.GREEN, colors.NORMAL))
        if not self.options.wait:
            self.logger.info("Task name: %s" % uniquerequestname)
            self.logger.info("Please use 'crab status' to check how the submission process proceeds.")

        if self.options.wait:
            self.checkStatusLoop(server, uniquerequestname)

        self.logger.debug("About to return")

        return {'requestname' : self.requestname , 'uniquerequestname' : uniquerequestname }


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( "-c","--config",
                                 dest = "config",
                                 default = None,
                                 help = "CRAB configuration file.",
                                 metavar = "FILE" )

        self.parser.add_option( "--wait,",
                                action="store_true",
                                dest="wait",
                                help="Continuously checking for job status after submitting.",
                                default=False )



    def validateOptions(self):
        """
        After doing the general options validation from the parent SubCommand class,
        do the validation of options that are specific to the submit command.
        """

        ## First call validateOptions() from the SubCommand class.
        SubCommand.validateOptions(self)
        ## If no configuration file was passed as an option, try to extract it from the arguments.
        ## Assume that the arguments can only be:
        ##     1) the configuration file name, and
        ##     2) parameters to override in the configuration file.
        ## The last ones should all contain an '=' sign, so these are not candidates to be the
        ## configuration file argument. Also, the configuration file name should end with '.py'.
        ## If can not find a configuration file candidate, use the default 'crabConfig.py'.
        ## If find more than one candidate, raise ConfigurationException.

        if self.options.config is None:
            use_default = True
            if len(self.args):
                config_candidates = [(arg,i) for i,arg in enumerate(self.args) if '=' not in arg and arg[-3:] == '.py']
                config_candidate_names = set([config_candidate_name for (config_candidate_name,_) in config_candidates])
                if len(config_candidate_names) == 1:
                    self.options.config = config_candidates[0][0]
                    del self.args[config_candidates[0][1]]
                    use_default = False
                elif len(config_candidate_names) > 1:
                    self.logger.info('Unable to unambiguously extract the configuration file from the command-line arguments.')
                    self.logger.info('Possible candidates are: %s' % list(config_candidate_names))
                    raise ConfigurationException('ERROR: Unable to extract configuration file from command-line arguments.')
            if use_default:
                self.options.config = 'crabConfig.py'


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there
        """
        valid, msg = SubCommand.validateConfig(self)
        if not valid:
            return False, msg

        ## Check that Data.unitsPerjob is specified.
        if hasattr(self.configuration.Data, 'unitsPerJob'):
            try:
                float(self.configuration.Data.unitsPerJob)
            except ValueError:
                msg = "Invalid CRAB configuration: Parameter Data.unitsPerJob must be a valid number, not %s." % (self.configuration.Data.unitsPerJob)
                return False, msg

        ## Check that JobType.pluginName and JobType.externalPluginFile are not both specified.
        if hasattr(self.configuration.JobType, 'pluginName') and hasattr(self.configuration.JobType, 'externalPluginFile'):
            msg = "Invalid CRAB configuration: Only one of JobType.pluginName or JobType.externalPluginFile parameters can be specified."
            pluginName_default = getParamDefaultValue('JobType.pluginName')
            if pluginName_default:
                msg += "\nIf neither JobType.pluginName nor JobType.externalPluginFile would be specified,"
                msg += " the default JobType.pluginName = '%s' would be used." % (pluginName_default)
            return False, msg
        ## Load the external plugin or check that the crab plugin is valid.
        external_plugin_name = getattr(self.configuration.JobType, 'externalPluginFile', None)
        crab_plugin_name = getattr(self.configuration.JobType, 'pluginName', None)
        crab_job_types = {'ANALYSIS': None, 'PRIVATEMC': None} #getJobTypes()
        if external_plugin_name:
            addPlugin(external_plugin_name) # Do we need to do this here?
        if crab_plugin_name:
            if upper(crab_plugin_name) not in crab_job_types:
                msg = "Invalid CRAB configuration: Parameter JobType.pluginName has an invalid value ('%s')." % (crab_plugin_name)
                msg += "\nAllowed values are: %s." % (", ".join(['%s' % job_type for job_type in crab_job_types.keys()]))
                return False, msg
            msg  = "Will use CRAB %s plugin" % ("Analysis" if upper(crab_plugin_name) == 'ANALYSIS' else "PrivateMC")
            msg += " (i.e. will run %s job type)." % ("an analysis" if upper(crab_plugin_name) == 'ANALYSIS' else "a MC generation")
            self.logger.debug(msg)

        ## Check that the particular combination (Data.publication = True, General.transferOutputs = False) is not specified.
        if getattr(self.configuration.Data, 'publication', getParamDefaultValue('Data.publication')) and \
           not getattr(self.configuration.General, 'transferOutputs', getParamDefaultValue('General.transferOutputs')):
            msg  = "Invalid CRAB configuration: Data.publication is True, but General.transferOutputs is False."
            msg += "\nPublication can not be performed if the output files are not transferred to a permanent storage."
            return False, msg

        ## Check that a storage site is specified if General.transferOutputs = True or General.transferLogs = True.
        if not hasattr(self.configuration.Site, 'storageSite'):
            if getattr(self.configuration.General, 'transferLogs', getParamDefaultValue('General.transferLogs')) or \
               getattr(self.configuration.General, 'transferOutputs', getParamDefaultValue('General.transferOutputs')):
                msg = "Invalid CRAB configuration: Parameter Site.storageSite is missing."
                return False, msg

        ## If an input dataset and a DBS URL are specified, check that the DBS URL is a good one.
        ## Also, if the DBS URL is 'phys0x', check that the input dataset tier is USER.
        if hasattr(self.configuration.Data, 'inputDBS'):
            if hasattr(self.configuration.Data, 'inputDataset'):
                msg = None
                dbs_urls_aliases = DBSURLS['reader'].keys()
                dbs_urls = DBSURLS['reader'].values()
                if (self.configuration.Data.inputDBS not in dbs_urls_aliases) and (self.configuration.Data.inputDBS.rstrip('/') not in dbs_urls):
                    msg  = "Invalid CRAB configuration: Parameter Data.inputDBS has an invalid value ('%s')." % (self.configuration.Data.inputDBS)
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['reader'].iteritems()])
                local_dbs_urls_aliases = ['phys01', 'phys02', 'phys03']
                local_dbs_urls = [DBSURLS['reader'][alias] for alias in local_dbs_urls_aliases if alias in DBSURLS['reader']]
                if self.configuration.Data.inputDBS in local_dbs_urls + local_dbs_urls_aliases:
                    inputDataset_parts = self.configuration.Data.inputDataset.split('/')
                    inputDataset_parts.pop(0)
                    inputDataset_tier = inputDataset_parts[-1] if len(inputDataset_parts) == 3 else None
                    user_data_tiers = ['USER']
                    if inputDataset_tier not in user_data_tiers:
                        msg  = "Invalid CRAB configuration: A local DBS instance '%s' was specified for reading an input dataset of tier %s." \
                               % (self.configuration.Data.inputDBS, inputDataset_tier)
                        msg += "\nDatasets of tier different than %s must be read from the global DBS instance; this is, set Data.inputDBS = 'global'." \
                               % (", ".join(user_data_tiers[:-1]) + " or " + user_data_tiers[-1] if len(user_data_tiers) > 1 else user_data_tiers[0])
                if msg:
                    inputDBS_default = getParamDefaultValue('Data.inputDBS')
                    if inputDBS_default:
                        inputDBS_default, inputDBS_default_alias = self.getDBSURLAndAlias(inputDBS_default, 'reader')
                        if inputDBS_default and inputDBS_default_alias:
                            msg += "\nIf Data.inputDBS would not be specified, the default '%s' ('%s') would be used." % (inputDBS_default_alias, inputDBS_default)
                    return False, msg

        ## If a publication DBS URL is specified and publication is ON, check that the DBS URL is a good one.
        if hasattr(self.configuration.Data, 'publishDBS'):
            if getattr(self.configuration.Data, 'publication', getParamDefaultValue('Data.publication')):
                dbs_urls = DBSURLS['writer'].values()
                dbs_urls_aliases = DBSURLS['writer'].keys()
                if (self.configuration.Data.publishDBS not in dbs_urls_aliases) and (self.configuration.Data.publishDBS.rstrip('/') not in dbs_urls):
                    msg  = "Invalid CRAB configuration: Parameter Data.publishDBS has an invalid value ('%s')." % (self.configuration.Data.publishDBS)
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['writer'].iteritems()])
                    publishDBS_default = getParamDefaultValue('Data.publishDBS')
                    if publishDBS_default:
                        publishDBS_default, publishDBS_default_alias = self.getDBSURLAndAlias(publishDBS_default, 'writer')
                        if publishDBS_default and publishDBS_default_alias:
                            msg += "\nIf Data.publishDBS would not be specified, the default '%s' ('%s') would be used." \
                                 % (publishDBS_default_alias, publishDBS_default)
                    return False, msg

        if hasattr(self.configuration.JobType, 'scriptExe'):
            if not os.path.isfile(self.configuration.JobType.scriptExe):
                msg = "Cannot find the file %s specified in the JobType.scriptExe configuration parameter." % (self.configuration.JobType.scriptExe)
                return False, msg

        return True, "Valid configuration"


    def getDBSURLAndAlias(self, arg, dbs_type = 'reader'):
        if arg in DBSURLS[dbs_type].keys():
            return DBSURLS[dbs_type][arg], arg
        if arg.rstrip('/') in DBSURLS[dbs_type].values():
            for alias in DBSURLS[dbs_type].keys():
                if DBSURLS[dbs_type][alias] == arg.rstrip('/'):
                    return DBSURLS[dbs_type][alias], alias
        return None, None

    ## TODO: This method is shared with resubmit. Put it in a common place.
    def _encodeRequest(self, configreq, listParams):
        """ Used to encode the request from a dict to a string. Include the code needed for transforming lists in the format required by
            cmsweb, e.g.:   adduserfiles = ['file1','file2']  ===>  [...]adduserfiles=file1&adduserfiles=file2[...]
        """
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam]) > 0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join( map(urllib.quote, configreq[lparam]) )
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        return str(encoded)


    def checkStatusLoop(self,server,uniquerequestname):
        self.logger.info("Waiting for task to be processed")

        maxwaittime= 900 #in second, changed to 15 minute max wait time, the original 1 hour is too long
        starttime=currenttime=time.time()
        endtime=currenttime+maxwaittime

        startimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime))
        endtimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime))

        self.logger.debug("Start time:%s" % startimestring )
        self.logger.debug("Max wait time: %s s until : %s" % (maxwaittime,endtimestring))

        #self.logger.debug('Looking up detailed status of task %s' % uniquerequestname)

        continuecheck=True
        tmpresult=None
        self.logger.info("Checking task status")

        while continuecheck:
            currenttime=time.time()
            querytimestring=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(currenttime))

            self.logger.debug('Looking up detailed status of task %s' % uniquerequestname)

            dictresult, status, reason = server.get(self.uri, data = { 'workflow' : uniquerequestname})
            dictresult = dictresult['result'][0]

            if status != 200:
                self.logger.info("The task has been submitted, \nImpossible to check task status now. \nPlease check again later by using: crab status -d <crab project directory>")
                msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(uniquerequestname), str(dictresult), str(reason))
                raise RESTCommunicationException(msg)

            self.logger.debug("Query Time:%s Task status:%s" %(querytimestring, dictresult['status']))

            if  dictresult['status'] != tmpresult:
                self.logger.info("Task status:%s" % dictresult['status'])
                tmpresult = dictresult['status']

                if dictresult['status'] == 'FAILED':
                    continuecheck = False
                    self.logger.info("%sError%s: The submission of your task failed. Please use 'crab status -d <crab project directory>' to get the error message" % (colors.RED, colors.NORMAL))
                elif dictresult['status'] == 'SUBMITTED' or dictresult['status'] == 'UNKNOWN': #untile the node_state file is available status is unknown
                    continuecheck = False
                    self.logger.info("%sSuccess%s: Your task has been processed and your jobs have been submitted successfully" % (colors.GREEN, colors.NORMAL))
                elif dictresult['status'] in ['NEW','HOLDING','QUEUED']:
                    self.logger.info("Please wait...")
                    time.sleep(30) #the original 60 second query time is too long
                else:
                    continuecheck = False
                    self.logger.info("Please check crab.log ")
                    self.logger.debug("CRABS Status other than FAILED,SUBMITTED,NEW,HOLDING,QUEUED")

            if currenttime > endtime:
                continuecheck = False
                self.logger.info("Exceed maximum query time \n Please check again later by using: crab status -d <crab project directory>")
                waittime=currenttime-starttime
                self.logger.debug("Wait time:%s" % waittime)
                break
        print '\a' #Generate audio bell
        self.logger.debug("Ended submission process")

