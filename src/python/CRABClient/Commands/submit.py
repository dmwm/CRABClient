"""
This is simply taking care of job submission
"""
import json, os
from string import upper
import types
import imp
import urllib
import time

from RESTInteractions import HTTPRequests

from CRABClient.Commands.SubCommand import SubCommand
from CRABClient import SpellChecker
from CRABClient.client_exceptions import MissingOptionException, ConfigurationException, RESTCommunicationException
from CRABClient.client_utilities import getJobTypes, createCache, addPlugin, server_info, colors ,getUrl

from CRABClient.ClientMapping import parameters_mapping, getParamDefaultValue
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


    def __init__(self, logger, cmdargs = []):
        SubCommand.__init__(self, logger, cmdargs, disable_interspersed_args = True)


    def __call__(self):
        valid = False
        configmsg = 'Default'

        self.logger.debug("Started submission")
        ######### Check if the user provided unexpected parameters ########
        #init the dictionary with all the known parameters
        all_config_params = [x for x in parameters_mapping['other-config-params']]
        for _, val in parameters_mapping['on-server'].iteritems():
            if val['config']:
                all_config_params.extend(val['config'])
        SpellChecker.DICTIONARY = SpellChecker.train(all_config_params)
        #iterate on the parameters provided by the user
        for section in self.configuration.listSections_():
            for attr in getattr(self.configuration, section).listSections_():
                par = (section + '.' + attr)
                #if the parameter is not know exit, but try to correct it before
                if not SpellChecker.is_correct( par ):
                    msg = 'The parameter %s is not known.' % par
                    msg += '' if SpellChecker.correct(par) == par else ' Did you mean %s?' % SpellChecker.correct(par)
                    raise ConfigurationException(msg)

        #usertarball and cmsswconfig use this parameter and we should set it up in a correct way
        self.configuration.General.serverUrl = self.serverurl

        uniquerequestname = None

        self.logger.debug("Working on %s" % str(self.requestarea))

        configreq = {}
        for param in parameters_mapping['on-server']:
            mustbetype = getattr(types, parameters_mapping['on-server'][param]['type'])
            default = parameters_mapping['on-server'][param]['default']
            config_params = parameters_mapping['on-server'][param]['config']
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
            elif param in ['savelogsflag', 'publication', 'nonprodsw', 'ignorelocality', 'saveoutput', 'oneEventMode']:
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

        jobconfig = {}
        self.configuration.JobType.proxyfilename = self.proxyfilename
        self.configuration.JobType.capath = HTTPRequests.getCACertPath()
        #get the backend URLs from the server external configuration
        serverBackendURLs = server_info('backendurls', self.serverurl, self.proxyfilename, getUrl(self.instance, resource='info'))
        #if cacheSSL is specified in the server external configuration we will use it to upload the sandbox (baseURL will be ignored)
        self.configuration.JobType.filecacheurl = serverBackendURLs['cacheSSL'] if 'cacheSSL' in serverBackendURLs else None
        pluginParams = [ self.configuration, self.logger, os.path.join(self.requestarea, 'inputs') ]
        crab_job_types = getJobTypes()
        if upper(configreq['jobtype']) in crab_job_types:
            plugjobtype = crab_job_types[upper(configreq['jobtype'])](*pluginParams)
            inputfiles, jobconfig, isbchecksum = plugjobtype.run(configreq)
        else:
            fullname = configreq['jobtype']
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            inputfiles, jobconfig, isbchecksum = pluginInst.run(configreq)

        if configreq['publication']:
            non_edm_files = jobconfig['tfileoutfiles'] + jobconfig['addoutputfiles']
            if non_edm_files:
                msg = "%sWARNING%s: The following output files will not be published, as they are not EDM files: %s" % (colors.RED, colors.NORMAL, non_edm_files)
                self.logger.warning(msg)

        if not configreq['publishname']:
            configreq['publishname'] =  isbchecksum
        else:
            configreq['publishname'] = "%s-%s" %(configreq['publishname'], isbchecksum)
        configreq.update(jobconfig)

        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str( configreq ) )

        dictresult, status, reason = server.put( self.uri, data = self._encodeRequest(configreq) )
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
            self.logger.info("Please use 'crab status' to check how the submission process proceed")

        if self.options.wait:
            self.checkStatusLoop(server,uniquerequestname)

        return uniquerequestname


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

        ## Check that the configuration object has the sections we expect it to have.
        ## (WMCore already checks that attributes added to the configuration object are of type ConfigSection.)
        ## Even if not all configuration sections need to be there, we anyway request
        ## the user to add all the sections in the configuration file.
        if not hasattr(self.configuration, 'General'):
            msg = "Crab configuration problem: Section 'General' is missing"
            return False, msg
        if not hasattr(self.configuration, 'JobType'):
            msg = "Crab configuration problem: Section 'JobType' is missing"
            return False, msg
        if not hasattr(self.configuration, 'Data'):
            msg = "Crab configuration problem: Section 'Data' is missing"
            return False, msg
        if not hasattr(self.configuration, 'Site'):
            msg = "Crab configuration problem: Section 'Site' is missing"
            return False, msg

        ## Check that Data.unitsPerjob is specified.
        if hasattr(self.configuration.Data, 'unitsPerJob'):
            try:
                float(self.configuration.Data.unitsPerJob)
            except ValueError:
                msg = "Crab configuration problem: Parameter Data.unitsPerJob must be a valid number, not %s" % self.configuration.Data.unitsPerJob
                return False, msg

        ## Check that JobType.pluginName and JobType.externalPluginFile are not both specified.
        if hasattr(self.configuration.JobType, 'pluginName') and hasattr(self.configuration.JobType, 'externalPluginFile'):
            msg = "Crab configuration problem: Only one of JobType.pluginName or JobType.externalPluginFile parameters can be specified"
            pluginName_default = getParamDefaultValue('JobType.pluginName')
            if pluginName_default:
                msg += "\nIf neither JobType.pluginName nor JobType.externalPluginFile would be specified, the default JobType.pluginName = '%s' would be used" \
                       % pluginName_default
            return False, msg
        ## Load the external plugin or check that the crab plugin is valid.
        external_plugin_name = getattr(self.configuration.JobType, 'externalPluginFile', None)
        crab_plugin_name = getattr(self.configuration.JobType, 'pluginName', None)
        crab_job_types = {'ANALYSIS': None, 'PRIVATEMC': None} #getJobTypes()
        if external_plugin_name:
            addPlugin(external_plugin_name) # Do we need to do this here?
        if crab_plugin_name and upper(crab_plugin_name) not in crab_job_types:
            msg = "Crab configuration problem: Parameter JobType.pluginName has an invalid value '%s'" % crab_plugin_name
            msg += "\nAllowed values are: %s" % ", ".join(['%s' % job_type for job_type in crab_job_types.keys()])
            return False, msg

        ## Check that the particular combination (Data.publication = True, General.transferOutput = False) is not specified.
        if hasattr(self.configuration.Data, 'publication') and hasattr(self.configuration.General, 'transferOutput'):
            if self.configuration.Data.publication and not self.configuration.General.transferOutput:
                msg  = "Crab configuration problem: Data.publication is on, but General.transferOutput is off"
                msg += "\nPublication can not be performed if the output files are not transferred to a permanent storage"
                return False, msg

        ## Check that a storage site is specified if General.transferOutput = True or General.saveLogs = True.
        if not hasattr(self.configuration.Site, 'storageSite'):
            if (hasattr(self.configuration.General, 'transferOutput') and self.configuration.General.transferOutput) or \
               (hasattr(self.configuration.General, 'saveLogs') and self.configuration.General.saveLogs):
                msg = "Crab configuration problem: Parameter Site.storageSite is missing"
                return False, msg

        ## If an input dataset and a DBS URL are specified, check that the DBS URL is a good one.
        ## Also, if the DBS URL is 'phys0x', check that the input dataset tier is USER.
        if hasattr(self.configuration.Data, 'dbsUrl'):
            if hasattr(self.configuration.Data, 'inputDataset'):
                msg = None
                dbs_urls_aliases = DBSURLS['reader'].keys()
                dbs_urls = DBSURLS['reader'].values()
                if (self.configuration.Data.dbsUrl not in dbs_urls_aliases) and (self.configuration.Data.dbsUrl.rstrip('/') not in dbs_urls):
                    msg  = "Crab configuration problem: Parameter Data.dbsUrl has an invalid value '%s'" % self.configuration.Data.dbsUrl
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['reader'].iteritems()])
                local_dbs_urls_aliases = ['phys01', 'phys02', 'phys03']
                local_dbs_urls = [DBSURLS['reader'][alias] for alias in local_dbs_urls_aliases if alias in DBSURLS['reader']]
                if self.configuration.Data.dbsUrl in local_dbs_urls + local_dbs_urls_aliases:
                    inputDataset_parts = self.configuration.Data.inputDataset.split('/')
                    inputDataset_parts.pop(0)
                    inputDataset_tier = inputDataset_parts[-1] if len(inputDataset_parts) == 3 else None
                    user_data_tiers = ['USER']
                    if inputDataset_tier not in user_data_tiers:
                        msg  = "Crab configuration problem: A local DBS instance '%s' was specified for reading an input dataset of tier %s" \
                               % (self.configuration.Data.dbsUrl, inputDataset_tier)
                        msg += "\nDatasets of tier different than %s must be read from the global DBS instance; this is, set Data.dbsUrl = 'global'" \
                               % (", ".join(user_data_tiers[:-1]) + " or " + user_data_tiers[-1] if len(user_data_tiers) > 1 else user_data_tiers[0])
                if msg:
                    dbsUrl_default = getParamDefaultValue('Data.dbsUrl')
                    if dbsUrl_default:
                        dbsUrl_default, dbsUrl_default_alias = self.getDBSURLAndAlias(dbsUrl_default, 'reader')
                        if dbsUrl_default and dbsUrl_default_alias:
                            msg += "\nIf Data.dbsUrl would not be specified, the default '%s' ('%s') would be used" % (dbsUrl_default_alias, dbsUrl_default)
                    return False, msg

        ## If a publication DBS URL is specified and publication is ON, check that the DBS URL is a good one.
        if hasattr(self.configuration.Data, 'publishDbsUrl'):
            publication_default = getParamDefaultValue('Data.publication')
            if getattr(self.configuration.Data, 'publication', publication_default):
                dbs_urls = DBSURLS['writer'].values()
                dbs_urls_aliases = DBSURLS['writer'].keys()
                if (self.configuration.Data.publishDbsUrl not in dbs_urls_aliases) and (self.configuration.Data.publishDbsUrl.rstrip('/') not in dbs_urls):
                    msg  = "Crab configuration problem: Parameter Data.publishDbsUrl has an invalid value '%s'" % self.configuration.Data.publishDbsUrl
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['writer'].iteritems()])
                    publishDbsUrl_default = getParamDefaultValue('Data.publishDbsUrl')
                    if publishDbsUrl_default:
                        publishDbsUrl_default, publishDbsUrl_default_alias = self.getDBSURLAndAlias(publishDbsUrl_default, 'writer')
                        if publishDbsUrl_default and publishDbsUrl_default_alias:
                            msg += "\nIf Data.publishDbsUrl would not be specified, the default '%s' ('%s') would be used" \
                                 % (publishDbsUrl_default_alias, publishDbsUrl_default)
                    return False, msg

        return True, "Valid configuration"


    def getDBSURLAndAlias(self, arg, dbs_type = 'reader'):
        if arg in DBSURLS[dbs_type].keys():
            return DBSURLS[dbs_type][arg], arg
        if arg in DBSURLS[dbs_type].values():
            for alias in DBSURLS[dbs_type].keys():
                if DBSURLS[dbs_type][alias] == arg.rstrip("/"):
                    return arg.rstrip("/"), alias
        return None, None


    def _encodeRequest(self, configreq):
        """ Used to encode the request from a dict to a string. Include the code needed for transforming lists in the format required by
            cmsweb, e.g.:   adduserfiles = ['file1','file2']  ===>  [...]adduserfiles=file1&adduserfiles=file2[...]
        """
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist',
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis', 'userfiles']
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam])>0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join( map(urllib.quote, configreq[lparam]) )
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        self.logger.debug('Encoded submit request: %s' % encoded)
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
                self.logger.info("The task has been submitted, \nImpossible to check task status now. \nPlease check again later by using: crab status -t  <Task Name>")
                msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(uniquerequestname), str(dictresult), str(reason))
                raise RESTCommunicationException(msg)

            self.logger.debug("Query Time:%s Task status:%s" %(querytimestring, dictresult['status']))

            if  dictresult['status'] != tmpresult:
                self.logger.info("Task status:%s" % dictresult['status'])
                tmpresult = dictresult['status']

                if dictresult['status'] == 'FAILED':
                    continuecheck = False
                    self.logger.info(self.logger.info("%sError%s: The submission of your task failed. Please use 'crab status -t <Task Name>' to get the error message" %(colors.RED, colors.NORMAL)))
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
                self.logger.info("Exceed max query time \n Please check again later by using: crab status -t  <Task Name>")
                waittime=currenttime-starttime
                self.logger.debug("Wait time:%s" % waittime)
                break

        self.logger.debug("Ended submission process")

