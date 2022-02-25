"""
This is simply taking care of job submission
"""
import os
import sys
import json
#import types  # not py3 compatible, see https://github.com/dmwm/CRABClient/issues/5004
import re
import shlex
import shutil
import tarfile
import tempfile
if sys.version_info >= (3, 0):
    from urllib.parse import urlencode, quote  # pylint: disable=E0611
if sys.version_info < (3, 0):
    from urllib import urlencode, quote

from CRABClient.ClientUtilities import DBSURLS
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientMapping import parametersMapping, getParamDefaultValue
from CRABClient.ClientExceptions import ClientException, RESTCommunicationException
from CRABClient.ClientUtilities import getJobTypes, createCache, addPlugin, server_info, colors,\
    setSubmitParserOptions, validateSubmitOptions, checkStatusLoop, execute_command

from ServerUtilities import MAX_MEMORY_PER_CORE, MAX_MEMORY_SINGLE_CORE, downloadFromS3, FEEDBACKMAIL

class submit(SubCommand):
    """
    Perform the submission to the CRABServer
    """

    shortnames = ['sub']


    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs, disable_interspersed_args=True)

        self.configreq = None
        self.configreq_encoded = None


    def __call__(self):
        self.logger.debug("Started submission")
        uniquerequestname = None

        self.logger.debug("Working on %s" % str(self.requestarea))

        self.configreq = {'dryrun': 1 if self.options.dryrun else 0}
        for param in parametersMapping['on-server']:
            #mustbetype = getattr(types, parametersMapping['on-server'][param]['type'])
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
                    self.configreq[param] = temp
                    break
                elif default is not None:
                    self.configreq[param] = default
                    temp = default
                else:
                    ## Parameter not strictly required.
                    pass
            ## Check that the requestname is of the right type.
            ## This is not checked in SubCommand.validateConfig().
            if param == 'workflow':
                #if isinstance(self.requestname, mustbetype):
                self.configreq['workflow'] = self.requestname
            ## Translate boolean flags into integers.
            elif param in ['savelogsflag', 'publication', 'publishgroupname', 'nonprodsw', 'useparent',\
                           'ignorelocality', 'saveoutput', 'oneEventMode', 'nonvaliddata', 'ignoreglobalblacklist']:
                self.configreq[param] = 1 if temp else 0
            ## Translate DBS URL aliases into DBS URLs.
            elif param in ['dbsurl', 'publishdbsurl']:
                if param == 'dbsurl':
                    dbstype = 'reader'
                elif param == 'publishdbsurl':
                    dbstype = 'writer'
                allowed_dbsurls = DBSURLS[dbstype].values()
                allowed_dbsurls_aliases = DBSURLS[dbstype].keys()
                if self.configreq[param] in allowed_dbsurls_aliases:
                    self.configreq[param] = DBSURLS[dbstype][self.configreq[param]]
                elif self.configreq[param].rstrip('/') in allowed_dbsurls:
                    self.configreq[param] = self.configreq[param].rstrip('/')
            elif param == 'scriptexe' and 'scriptexe' in self.configreq:
                self.configreq[param] = os.path.basename(self.configreq[param])

        jobconfig = {}
        #get the backend URLs from the server external configuration

        serverBackendURLs = server_info(crabserver=self.crabserver, subresource='backendurls')
        #if cacheSSL is specified in the server external configuration we will use it to upload the sandbox
        filecacheurl = serverBackendURLs['cacheSSL'] if 'cacheSSL' in serverBackendURLs else None
        pluginParams = [self.configuration, self.proxyfilename, self.logger,
                        os.path.join(self.requestarea, 'inputs'), self.crabserver, self.s3tester]
        crab_job_types = getJobTypes()
        if self.configreq['jobtype'].upper() in crab_job_types:
            plugjobtype = crab_job_types[self.configreq['jobtype'].upper()](*pluginParams)
            dummy_inputfiles, jobconfig = plugjobtype.run(filecacheurl)
        else:
            fullname = self.configreq['jobtype']
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            dummy_inputfiles, jobconfig = pluginInst.run()

        if self.configreq['publication']:
            non_edm_files = jobconfig['tfileoutfiles'] + jobconfig['addoutputfiles']
            if non_edm_files:
                msg = "%sWarning%s: The following output files will not be published, as they are not EDM files: %s"\
                      % (colors.RED, colors.NORMAL, non_edm_files)
                self.logger.warning(msg)

        self.configreq.update(jobconfig)
        server = self.crabserver

        self.logger.info("Sending the request to the server at %s" % self.serverurl)
        self.logger.debug("Submitting %s " % str(self.configreq))
        ## TODO: this shouldn't be hard-coded.
        listParams = ['addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist', \
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis', 'userfiles', 'scriptargs', 'extrajdl']
        self.configreq_encoded = self._encodeRequest(self.configreq, listParams)
        self.logger.debug('Encoded submit request: %s' % (self.configreq_encoded))

        dictresult, status, reason = server.put(api=self.defaultApi, data=self.configreq_encoded)
        self.logger.debug("Result: %s" % dictresult)
        if status != 200:
            msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(self.configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        elif 'result' in dictresult:
            uniquerequestname = dictresult["result"][0]["RequestName"]
        else:
            msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
                   % (str(self.configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        tmpsplit = self.serverurl.split(':')
        createCache(self.requestarea, tmpsplit[0], tmpsplit[1] if len(tmpsplit) > 1 else '', uniquerequestname,
                    voRole=self.voRole, voGroup=self.voGroup, instance=self.instance,
                    originalConfig=self.configuration)

        self.logger.info("%sSuccess%s: Your task has been delivered to the %s CRAB3 server." % (colors.GREEN, colors.NORMAL, self.instance))
        self.logger.info("Task name: %s" % uniquerequestname)
        projDir = os.path.join(getattr(self.configuration.General, 'workArea', '.'), self.requestname)
        self.logger.info("Project dir: %s" % projDir)
        if not (self.options.wait or self.options.dryrun):
            self.logger.info("Please use ' crab status -d %s ' to check how the submission process proceeds.", projDir)
        else:
            targetTaskStatus = 'UPLOADED' if self.options.dryrun else 'SUBMITTED'
            checkStatusLoop(self.logger, server, self.defaultApi, uniquerequestname, targetTaskStatus, self.name)

        if self.options.dryrun:
            self.printDryRunResults(*self.executeTestRun(filecacheurl, uniquerequestname))

        self.logger.debug("About to return")

        return {'requestname':self.requestname, 'uniquerequestname':uniquerequestname}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        setSubmitParserOptions(self.parser)


    def validateConfigOption(self):
        """
        After doing the general options validation from the parent SubCommand class,
        do the validation of options that are specific to the submit command.
        """
        validateSubmitOptions(self.options, self.args)


    def validateConfig(self):
        """
        __validateConfig__

        Checking if needed input parameters are there
        """
        valid, msg = SubCommand.validateConfig(self)
        if not valid:
            return False, msg

        requestNameLenLimit = 100
        if hasattr(self.configuration.General, 'requestName'):
            if len(self.configuration.General.requestName) > requestNameLenLimit:
                msg = "Invalid CRAB configuration: Parameter General.requestName should not be longer than %d characters." % (requestNameLenLimit)
                return False, msg

        splitting = getattr(self.configuration.Data, 'splitting', 'Automatic')
        autoSplitt = True if splitting == 'Automatic' else False
        autoSplittUnitsMin = 180 # 3 hours (defined also in TW config as 'minAutomaticRuntimeMins')
        autoSplittUnitsMax = 2700 # 45 hours
        ## Check that maxJobRuntimeMin is not used with Automatic splitting
        if autoSplitt and hasattr(self.configuration.JobType, 'maxJobRuntimeMin'):
            msg = "The 'maxJobRuntimeMin' parameter is not compatible with the 'Automatic' splitting mode (default)."
            return False, msg

        ## Check that --dryrun is not used with Automatic splitting
        if autoSplitt and self.options.dryrun:
            msg = "The 'dryrun' option is not compatible with the 'Automatic' splitting mode (default)."
            return False, msg

        ## Check that Data.unitsPerjob is specified.
        if hasattr(self.configuration.Data, 'unitsPerJob'):
            try:
                float(self.configuration.Data.unitsPerJob)
            except ValueError:
                msg = "Invalid CRAB configuration: Parameter Data.unitsPerJob must be a valid number, not %s." % (self.configuration.Data.unitsPerJob)
                return False, msg
            if not int(self.configuration.Data.unitsPerJob) > 0:
                msg = "Invalid CRAB configuration: Parameter Data.unitsPerJob must be > 0, not %s." % (self.configuration.Data.unitsPerJob)
                return False, msg
            if autoSplitt and (self.configuration.Data.unitsPerJob > autoSplittUnitsMax  or  self.configuration.Data.unitsPerJob < autoSplittUnitsMin):
                msg = "Invalid CRAB configuration: In case of Automatic splitting, the Data.unitsPerJob parameter must be in the [%d, %d] minutes range. You asked for %d minutes." % (autoSplittUnitsMin, autoSplittUnitsMax, self.configuration.Data.unitsPerJob)
                return False, msg
        elif not autoSplitt:
            # The default value is only valid for automatic splitting!
            msg = "Invalid CRAB configuration: Parameter Data.unitsPerJob is mandatory for '%s' splitting mode." % splitting
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
        crab_job_types = {'ANALYSIS': None, 'PRIVATEMC': None, 'COPYCAT': None} #getJobTypes()
        if external_plugin_name:
            addPlugin(external_plugin_name) # Do we need to do this here?
        if crab_plugin_name:
            if crab_plugin_name.upper() not in crab_job_types:
                msg = "Invalid CRAB configuration: Parameter JobType.pluginName has an invalid value ('%s')." % (crab_plugin_name)
                msg += "\nAllowed values are: %s." % (", ".join(['%s' % job_type for job_type in crab_job_types.keys()]))
                return False, msg
            msg = "Will use CRAB %s plugin" % ("Analysis" if crab_plugin_name.upper() == 'ANALYSIS' else "PrivateMC")
            msg += " (i.e. will run %s job type)." % ("an analysis" if crab_plugin_name.upper() == 'ANALYSIS' else "a MC generation")
            self.logger.debug(msg)

        ## Check that the requested memory does not exceed the allowed maximum.
        nCores = getattr(self.configuration.JobType, 'numCores', 1)
        absMaxMemory = max(MAX_MEMORY_SINGLE_CORE, nCores*MAX_MEMORY_PER_CORE)
        self.defaultMaxMemory = parametersMapping['on-server']['maxmemory']['default']  # pylint: disable=attribute-defined-outside-init
        self.maxMemory = getattr(self.configuration.JobType, 'maxMemoryMB', self.defaultMaxMemory)  # pylint: disable=attribute-defined-outside-init
        if self.maxMemory > absMaxMemory:
            msg = "Task requests %s MB of memory, above the allowed maximum of %s" % (self.maxMemory, absMaxMemory)
            msg += " for a %d core(s) job.\n" % nCores
            return False, msg

        ## Check that the particular combination (Data.publication = True, General.transferOutputs = False) is not specified.
        if getattr(self.configuration.Data, 'publication', getParamDefaultValue('Data.publication')) and \
           not getattr(self.configuration.General, 'transferOutputs', getParamDefaultValue('General.transferOutputs')):
            msg = "Invalid CRAB configuration: Data.publication is True, but General.transferOutputs is False."
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
                    msg = "Invalid CRAB configuration: Parameter Data.inputDBS has an invalid value ('%s')." % (self.configuration.Data.inputDBS)
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['reader'].items()])
                local_dbs_urls_aliases = ['phys01', 'phys02', 'phys03']
                local_dbs_urls = [DBSURLS['reader'][alias] for alias in local_dbs_urls_aliases if alias in DBSURLS['reader']]
                if self.configuration.Data.inputDBS in local_dbs_urls + local_dbs_urls_aliases:
                    inputDataset_parts = self.configuration.Data.inputDataset.split('/')
                    inputDataset_parts.pop(0)
                    inputDataset_tier = inputDataset_parts[-1] if len(inputDataset_parts) == 3 else None
                    user_data_tiers = ['USER']
                    if inputDataset_tier not in user_data_tiers:
                        msg = "Invalid CRAB configuration: A local DBS instance '%s' was specified for reading an input dataset of tier %s." \
                               % (self.configuration.Data.inputDBS, inputDataset_tier)
                        msg += "\nDatasets of tier different than %s must be read from the global DBS instance; this is, set Data.inputDBS = 'global'." \
                               % (", ".join(user_data_tiers[:-1]) + " or " + user_data_tiers[-1] if len(user_data_tiers) > 1 else user_data_tiers[0])
                if msg:
                    inputDBS_default = getParamDefaultValue('Data.inputDBS')
                    if inputDBS_default:
                        inputDBS_default, inputDBS_default_alias = self.getDBSURLAndAlias(inputDBS_default, 'reader')
                        if inputDBS_default and inputDBS_default_alias:
                            msg += "\nIf Data.inputDBS would not be specified, the default '%s' ('%s') would be used." \
                                   % (inputDBS_default_alias, inputDBS_default)
                    return False, msg

        ## If a publication DBS URL is specified and publication is ON, check that the DBS URL is a good one.
        if hasattr(self.configuration.Data, 'publishDBS'):
            if getattr(self.configuration.Data, 'publication', getParamDefaultValue('Data.publication')):
                dbs_urls = DBSURLS['writer'].values()
                dbs_urls_aliases = DBSURLS['writer'].keys()
                if (self.configuration.Data.publishDBS not in dbs_urls_aliases) and (self.configuration.Data.publishDBS.rstrip('/') not in dbs_urls):
                    msg = "Invalid CRAB configuration: Parameter Data.publishDBS has an invalid value ('%s')." % (self.configuration.Data.publishDBS)
                    msg += "\nAllowed values are: "
                    msg += "\n                    ".join(["'%s' ('%s')" % (alias, url) for alias, url in DBSURLS['writer'].items()])
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
        ## If ignoreLocality is set, check that a sitewhilelist is present
        if getattr(self.configuration.Data, 'ignoreLocality', False):
            if not hasattr(self.configuration.Site, 'whitelist'):
                msg = "Invalid CRAB configuration:\n when ignoreLocality is set a valid site white list must be specified using the Site.whitelist parameter"
                return False, msg

        if hasattr(self.configuration.General, 'failureLimit'):
            msg = "You have specified deprecated parameter 'failureLimit' which will be removed in the near future."
            msg += "\nIf you really need it write a mail to %s explaining your use case." % FEEDBACKMAIL
            self.logger.warning("%sWARNING%s: %s" % (colors.RED, colors.NORMAL, msg))

        return True, "Valid configuration"


    def getDBSURLAndAlias(self, arg, dbs_type='reader'):
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
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join(map(quote, configreq[lparam]))
                del configreq[lparam]
        encoded = urlencode(configreq) + encodedLists
        return str(encoded)


    def executeTestRun(self, filecacheurl, uniquerequestname):
        """
        Downloads the dry run tarball from the User File Cache and unpacks it in a temporary directory.
        Runs a trial to obtain the performance report. Repeats trial with successively larger input events
        until a job length of maxSeconds is reached (this improves accuracy for fast-running CMSSW parameter sets.)
        """
        cwd = os.getcwd()
        try:
            tmpDir = tempfile.mkdtemp()
            self.logger.info('Created temporary directory for dry run sandbox in %s' % tmpDir)
            os.chdir(tmpDir)
            downloadFromS3(crabserver=self.crabserver, filepath=os.path.join(tmpDir, 'dry-run-sandbox.tar.gz'),
                           objecttype='runtimefiles', taskname=uniquerequestname, logger=self.logger)
            for name in ['dry-run-sandbox.tar.gz', 'InputFiles.tar.gz', 'CMSRunAnalysis.tar.gz', 'sandbox.tar.gz']:
                tf = tarfile.open(os.path.join(tmpDir, name))
                tf.extractall(tmpDir)
                tf.close()
            os.environ.update({'CRAB3_RUNTIME_DEBUG': 'True', '_CONDOR_JOB_AD': 'Job.submit'})

            with open('splitting-summary.json') as f:
                splitting = json.load(f)

            if self.options.skipEstimates:
                return splitting, None

            self.logger.info('Executing test, please wait...')

            events = 10
            totalJobSeconds = 0
            maxSeconds = 25
            while totalJobSeconds < maxSeconds:
                optsList = getCMSRunAnalysisOpts('Job.submit', 'RunJobs.dag', job=1, events=events)
                # from a python list to a string which can be used as shell command argument
                opts = ''
                for opt in optsList:
                    opts = opts + ' %s' % opt
                # job wrapper needs to be executed in a clean shell, like it happens in the WN, not
                # inside the environemnt where CRABClient runs (i.e. some CMSSW env. which may conflict
                # with the WMCore code used in the wrapper
                undoScram = "eval `scram unsetenv -sh`; "
                command = undoScram + 'sh CMSRunAnalysis.sh ' + opts
                out, err, returncode = execute_command(command=command)
                self.logger.debug(out)
                if returncode != 0:
                    raise ClientException('Dry run failed to execute local test run:\n StdOut: %s\n StdErr: %s' % (out, err))

                #Once this https://github.com/dmwm/CRABServer/pull/4938 will get merged the job will be executed inside the CMSSW dir
                #Therefore the 'jobReport.json' will not be in the cwd. We will delete these three lines of code in the future
                jobReport = 'jobReport.json'
                if not os.path.isfile(jobReport):
                    jobReport = os.path.join(self.configreq["jobsw"], jobReport)
                with open(jobReport) as f:
                    report = json.load(f)['steps']['cmsRun']['performance']
                events += (maxSeconds / float(report['cpu']['AvgEventTime']))
                totalJobSeconds = float(report['cpu']['TotalJobTime'])

        finally:
            os.chdir(cwd)
            shutil.rmtree(tmpDir)

        return splitting, report

    def printDryRunResults(self, splitting, report):
        """
        Calculate the estimated total job length from the splitting results and performance report
        and print the results.
        """

        algos = {"Automatic": "seconds",
                 "LumiBased": "lumis",
                 "EventBased": "events",
                 "FileBased": "files",
                 "EventAwareLumiBased": "events"}

        estimates = {}
        quantities = {}
        units = algos[splitting['algo']]
        msg = ", with an estimated processing time of %i minutes"
        for x in ['total', 'max', 'min', 'avg']:
            quantities[x] = "%i %s" % (splitting['%s_%s' % (x, units)], units)
            estimates[x] = ''
            if not self.options.skipEstimates:
                secondsPerEvent = float(report['cpu']['AvgEventTime'])
                estimates[x] = msg % (secondsPerEvent * splitting['%s_events' % x] / 60)

        self.logger.info("\nUsing %s splitting" % splitting['algo'])
        self.logger.info("Task consists of %i jobs to process %s" % (splitting['total_jobs'], quantities['total']))
        self.logger.info("The longest job will process %s%s" % (quantities['max'], estimates['max']))
        self.logger.info("The average job will process %s%s" % (quantities['avg'], estimates['avg']))
        self.logger.info("The shortest job will process %s%s" % (quantities['min'], estimates['min']))

        if not self.options.skipEstimates:
            self.logger.info("The estimated memory requirement is %.0f MB" % float(report['memory']['PeakValueRss']))
            if float(report['memory']['PeakValueRss']) > self.maxMemory:
                msg = "\nWarning: memory estimate of %.0f MB exceeds what has been requested (JobType.maxMemoryMB = %i).\n"\
                    "Jobs which exceed JobType.maxMemoryMB will fail. Increasing JobType.maxMemoryMB more than 500 MB beyond \n"\
                    "the default of %i MB is not recommended, as fewer sites will be able to run your jobs. Please see\n"\
                    "https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideEDMTimingAndMemory for information\n"\
                    "about EDM Timing and Memory tools for checking the memory footprint of your CMSSW configuration."
                self.logger.warning(msg % (float(report['memory']['PeakValueRss']), self.maxMemory, self.defaultMaxMemory))

            self.logger.info("\nTiming quantities given below are ESTIMATES. Keep in mind that external factors\n"\
                             "such as transient file-access delays can reduce estimate reliability.")

            minJobs = 10
            targetSecondsPerJob = 8 * 3600
            threshold = 0.7
            tooFew = splitting['total_jobs'] < minJobs
            tooLong = secondsPerEvent * splitting['max_events'] > targetSecondsPerJob
            tooShort = secondsPerEvent * splitting['avg_events'] < targetSecondsPerJob * threshold
            if tooFew or tooLong or tooShort:
                eventsPerUnit = splitting['avg_events'] / splitting['avg_%s' % units]
                if tooFew:
                    eventsPerJob = splitting['total_%s' % units] * eventsPerUnit / minJobs
                    self.logger.info('\nAn update to your splitting parameters is recommended.')
                else:
                    eventsPerJob = targetSecondsPerJob / secondsPerEvent

                self.logger.info('\nFor ~%i minute jobs, use:' % (eventsPerJob * secondsPerEvent / 60))
                self.logger.info('Data.unitsPerJob = %i' % (eventsPerJob / eventsPerUnit))
                self.logger.info('You will need to submit a new task')
                if units == 'events':
                    self.logger.info('Data.totalUnits = %i' %  splitting['total_events'])

        self.logger.info("\nDry run requested: task paused\nTo continue processing, use 'crab proceed'\n")


def getCMSRunAnalysisOpts(ad, dag, job=1, events=10):
    """
    Parse the job ad to obtain the arguments that were passed to condor.
    """

    set_re = re.compile(r'\+?(\w+)\s*=\s*(.*)$')

    info = {}
    with open(ad) as f:
        for line in f:
            m = set_re.match(line)
            if not m:
                continue
            key, value = m.groups()
            # Somehow, Condor likes doubled double quotes?
            info[key] = value.strip("'\"").replace('""', '"')
    with open(dag) as f:
        for line in f:
            if line.startswith('VARS Job{job}'.format(job=job)):
                break
        else:
            raise ClientException('Dry run failed to execute parse DAG description.')
        for setting in shlex.split(line):
            m = set_re.match(setting)
            if not m:
                continue
            key, value = m.groups()
            info[key] = value.replace('""', '"')

    info.update({'CRAB_Id': '0', 'firstEvent': '1', 'lastEvent': str(int(events) + 1)})

    args = shlex.split(info['Arguments'])
    def repl(match):
        return info[match.group(1)]
    return [re.sub(r'\$\((\w+)\)', repl, arg) for arg in args]
