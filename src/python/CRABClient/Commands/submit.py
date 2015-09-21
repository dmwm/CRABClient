"""
This is simply taking care of job submission
"""
import os
import json
import types
import shutil
import urllib
import tarfile
import tempfile
import subprocess
from string import upper

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.ClientUtilities import DBSURLS
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientMapping import parametersMapping, getParamDefaultValue
from CRABClient.ClientExceptions import ClientException, RESTCommunicationException
from CRABClient.ClientUtilities import getJobTypes, createCache, addPlugin, server_info, colors, getUrl, setSubmitParserOptions, validateSubmitOptions, checkStatusLoop


class submit(SubCommand):
    """
    Perform the submission to the CRABServer
    """

    shortnames = ['sub']


    def __init__(self, logger, cmdargs=None):
        SubCommand.__init__(self, logger, cmdargs, disable_interspersed_args=True)


    def __call__(self):
        valid = False
        configmsg = 'Default'

        self.logger.debug("Started submission")
        serverFactory = CRABClient.Emulator.getEmulator('rest')

        uniquerequestname = None

        self.logger.debug("Working on %s" % str(self.requestarea))

        self.configreq = {'dryrun': 1 if self.options.dryrun else 0}
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
                if mustbetype == type(self.requestname):
                    self.configreq['workflow'] = self.requestname
            ## Translate boolean flags into integers.
            elif param in ['savelogsflag', 'publication', 'publishgroupname', 'nonprodsw', 'useparent', 'ignorelocality', 'saveoutput', 'oneEventMode', 'nonvaliddata']:
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
        serverBackendURLs = server_info('backendurls', self.serverurl, self.proxyfilename, getUrl(self.instance, resource='info'))
        #if cacheSSL is specified in the server external configuration we will use it to upload the sandbox (baseURL will be ignored)
        filecacheurl = serverBackendURLs['cacheSSL'] if 'cacheSSL' in serverBackendURLs else None
        pluginParams = [self.configuration, self.logger, os.path.join(self.requestarea, 'inputs')]
        crab_job_types = getJobTypes()
        if upper(self.configreq['jobtype']) in crab_job_types:
            plugjobtype = crab_job_types[upper(self.configreq['jobtype'])](*pluginParams)
            inputfiles, jobconfig = plugjobtype.run(filecacheurl)
        else:
            fullname = self.configreq['jobtype']
            basename = os.path.basename(fullname).split('.')[0]
            plugin = addPlugin(fullname)[basename]
            pluginInst = plugin(*pluginParams)
            inputfiles, jobconfig = pluginInst.run()

        if self.configreq['publication']:
            non_edm_files = jobconfig['tfileoutfiles'] + jobconfig['addoutputfiles']
            if non_edm_files:
                msg = "%sWarning%s: The following output files will not be published, as they are not EDM files: %s" % (colors.RED, colors.NORMAL, non_edm_files)
                self.logger.warning(msg)

        self.configreq.update(jobconfig)
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.info("Sending the request to the server")
        self.logger.debug("Submitting %s " % str(self.configreq))
        ## TODO: this shouldn't be hard-coded.
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist', \
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis', 'userfiles', 'scriptargs', 'extrajdl']
        self.configreq_encoded = self._encodeRequest(self.configreq, listParams)
        self.logger.debug('Encoded submit request: %s' % (self.configreq_encoded))

        dictresult, status, reason = server.put(self.uri, data = self.configreq_encoded)
        self.logger.debug("Result: %s" % dictresult)
        if status != 200:
            msg = "Problem sending the request:\ninput:%s\noutput:%s\nreason:%s" % (str(self.configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)
        elif dictresult.has_key("result"):
            uniquerequestname = dictresult["result"][0]["RequestName"]
        else:
            msg = "Problem during submission, no request ID returned:\ninput:%s\noutput:%s\nreason:%s" \
                   % (str(self.configreq), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        tmpsplit = self.serverurl.split(':')
        createCache(self.requestarea, tmpsplit[0], tmpsplit[1] if len(tmpsplit) > 1 else '', uniquerequestname,
                    voRole=self.voRole, voGroup=self.voGroup, instance=self.instance,
                    originalConfig=self.configuration)

        self.logger.info("%sSuccess%s: Your task has been delivered to the CRAB3 server." %(colors.GREEN, colors.NORMAL))
        if not (self.options.wait or self.options.dryrun):
            self.logger.info("Task name: %s" % uniquerequestname)
            self.logger.info("Please use 'crab status' to check how the submission process proceeds.")
        else:
            targetTaskStatus = 'UPLOADED' if self.options.dryrun else 'SUBMITTED'
            checkStatusLoop(self.logger, server, self.uri, uniquerequestname, targetTaskStatus, self.name)

        if self.options.dryrun:
            self.printDryRunResults(*self.executeTestRun(filecacheurl))

        self.logger.debug("About to return")

        return {'requestname': self.requestname , 'uniquerequestname': uniquerequestname}


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        setSubmitParserOptions(self.parser)


    def validateOptions(self):
        """
        After doing the general options validation from the parent SubCommand class,
        do the validation of options that are specific to the submit command.
        """
        ## First do the basic validation in the SubCommand.
        SubCommand.validateOptions(self)

        validateSubmitOptions(self.options, self.args, self.logger)


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
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join(map(urllib.quote, configreq[lparam]))
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        return str(encoded)


    def executeTestRun(self, filecacheurl):
        """
        Downloads the dry run tarball from the User File Cache and unpacks it in a temporary directory.
        Runs a trial to obtain the performance report. Repeats trial with successively larger input events
        until a job length of maxSeconds is reached (this improves accuracy for fast-running CMSSW parameter sets.)
        """
        ufc = CRABClient.Emulator.getEmulator('ufc')({'endpoint' : filecacheurl})
        cwd = os.getcwd()
        try:
            tmpDir = tempfile.mkdtemp()
            os.chdir(tmpDir)
            self.logger.info('Creating temporary directory for dry run sandbox in %s' % tmpDir)
            ufc.downloadLog('dry-run-sandbox.tar.gz', output=os.path.join(tmpDir, 'dry-run-sandbox.tar.gz'))
            for name in ['dry-run-sandbox.tar.gz', 'CMSRunAnalysis.tar.gz', 'sandbox.tar.gz']:
                tf = tarfile.open(os.path.join(tmpDir, name))
                tf.extractall(tmpDir)
                tf.close()
            env = os.environ.update({'CRAB3_RUNTIME_DEBUG': 'True', '_CONDOR_JOB_AD': 'Job.submit'})

            with open('splitting-summary.json') as f:
                splitting = json.load(f)

            if self.options.skipEstimates:
                return splitting, None

            self.logger.info('Executing test, please wait...')

            events = 10
            totalJobSeconds = 0
            maxSeconds = 25
            while totalJobSeconds < maxSeconds:
                opts = getCMSRunAnalysisOpts('Job.submit', 'RunJobs.dag', job=1, events=events)

                s = subprocess.Popen(['sh', 'CMSRunAnalysis.sh'] + opts, env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                out, err = s.communicate()
                self.logger.debug(out)
                if s.returncode != 0:
                    raise ClientException('Dry run failed to execute local test run:\n StdOut: %s\n StdErr: %s' % (out, err))

                #Once this https://github.com/dmwm/CRABServer/pull/4938 will get merged the job will be executed inside the CMSSW dir
                #Therefore the 'jobReport.json' will not be in the cwd. We will delete these three lines of code in the future
                jobReport = 'jobReport.json'
                if not os.path.isfile(jobReport):
                    jobReport = os.path.join( self.configreq["jobsw"], jobReport)
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

        algos = {"LumiBased": "lumis",
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
            defaultMaxMemory = parametersMapping['on-server']['maxmemory']['default']
            maxMemory = getattr(self.configuration.JobType, 'maxmemory', defaultMaxMemory)
            if float(report['memory']['PeakValueRss']) > maxMemory:
                msg = "\nWarning: memory estimate of %.0f MB exceeds what has been requested (JobType.maxMemoryMB = %i).\n"\
                    "Jobs which exceed JobType.maxMemoryMB will fail. Increasing JobType.maxMemoryMB more than 500 MB beyond \n"\
                    "the default of %i MB is not recommended, as fewer sites will be able to run your jobs. Please see\n"\
                    "https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideEDMTimingAndMemory for information\n"\
                    "about EDM Timing and Memory tools for checking the memory footprint of your CMSSW configuration."
                self.logger.warning(msg % (float(report['memory']['PeakValueRss']), maxMemory, defaultMaxMemory))

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

    info = {}
    with open(ad) as f:
        for key, val in [line.split('=', 1) for line in f if len(line.split('=', 1)) == 2]:
            val = val.strip().strip('"').strip("'")
            info[key.strip().replace('+', '')] = val
    with open(dag) as f:
        for line in f:
            if line.startswith('VARS Job{job}'.format(job=job)):
                break
        for entry in line.strip().replace(r'\"\"', '"').replace('", "', '","').split():
            parts = entry.split('=')
            if len(parts) == 2:
                info[parts[0]] = parts[1].strip('"')

    info.update({'CRAB_Id': '0', 'firstEvent': '1', 'lastEvent': str(int(events) + 1)})

    return [x.strip("'") % info for x in info['Arguments'].replace('$', '%').replace(')', ')s').split()]


