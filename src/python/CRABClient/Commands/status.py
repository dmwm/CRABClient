from __future__ import division # I want floating points
import urllib
import math

import CRABClient.Emulator
from CRABClient.ClientUtilities import colors
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import RESTCommunicationException, ConfigurationException
from CRABClient import __version__

def to_hms(val):
    s = val % 60
    val -= s
    val = (val / 60)
    m = val % 60
    val -= m
    h = val / 60
    return "%d:%02d:%02d" % (h, m, s)

PUBLICATION_STATES = {
    'not_published': 'idle',
    'publication_failed': 'failed',
    'published': 'finished',
    'publishing': 'running',
}


class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by the -d/--dir option.
    """

    shortnames = ['st']
    states = ['submitted', 'failure', 'queued', 'success']


    def _stateColor(self, state):
        if state == 'failed':
            return colors.RED
        elif state == 'running':
            return colors.GREEN
        elif state == 'idle' or state == 'unsubmitted':
            return colors.GRAY
        else:
            return colors.NORMAL


    def _percentageString(self, state, value, total):
        state = PUBLICATION_STATES.get(state, state)
        digit_count = int(math.ceil(math.log(max(value, total)+1, 10)))
        format_str = "%5.1f%% (%s%" + str(digit_count) + "d%s/%" + str(digit_count) + "d)"
        return format_str % ((value*100/total), self._stateColor(state), value, colors.NORMAL, total)


    def _printState(self, state, ljust):
        state = PUBLICATION_STATES.get(state, state)
        return ('{0}{1:<' + str(ljust) + '}{2}').format(self._stateColor(state), state, colors.NORMAL)


    def __call__(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        user = self.cachedinfo['RequestName'].split("_")[2].split(":")[-1]
        verbose = int(self.summary or self.long or self.json)
        if self.idle:
            verbose = 2
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName'], 'verbose': verbose })
        dictresult = dictresult['result'][0] #take just the significant part

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.printShort(dictresult, user)

        if 'jobs' not in dictresult:
            self.logger.info("\nNo jobs created yet!")
        else:
            if 'publication' in dictresult:
                self.printPublication(dictresult)
            self.printErrors(dictresult)
            # Note several options could be combined
            if self.summary:
                self.printSummary(dictresult)
            if self.long:
                self.printLong(dictresult)
            if self.idle:
                self.printIdle(dictresult, user)
            if self.json:
                self.logger.info(dictresult['jobs'])

        return dictresult


    def printShort(self, dictresult, username):
        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}

        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])

        self.logger.info("Task status:\t\t\t%s" % dictresult['status'])

        def logJDefErr(jdef):
            """Printing job def failures if any"""
            if jdef['jobdefErrors']:
                self.logger.error("%sFailed to inject %s\t%s out of %s:" % (colors.RED, colors.NORMAL, \
                                                                            jdef['failedJobdefs'], jdef['totalJobdefs']))
                for error in jdef['jobdefErrors']:
                    self.logger.info("\t%s" % error)

        ## Print the warning messages (these are the warnings in the Tasks DB,
        ## and/or maybe some warning added by the REST Interface to the status result).
        if dictresult['taskWarningMsg']:
            for warningMsg in dictresult['taskWarningMsg']:
                self.logger.warning("%sWarning:%s\t\t\t%s." % (colors.RED, colors.NORMAL, warningMsg))
        if dictresult['taskFailureMsg']:
            if dictresult['status'] == "FAILED":
                self.logger.error("%sError during task injection:%s\t%s" % (colors.RED,colors.NORMAL, dictresult['taskFailureMsg']))
            else:
                self.logger.error("%sError during task information retrieval:%s\t%s." % (colors.RED, colors.NORMAL, dictresult['taskFailureMsg']))
            # We might also have more information in the job def errors
            logJDefErr(jdef=dictresult)
        else:
            ## The REST Interface can return dictresult['jobSetID'] = '' or dictresult['jobSetID'] = task name.
            if self.cachedinfo['RequestName'] == dictresult['jobSetID']:
                ## Print the Dashboard and GlideMon monitoring URLs for this task.
                taskname = urllib.quote(dictresult['jobSetID'])
                glidemonURL = "http://glidemon.web.cern.ch/glidemon/jobs.php?taskname=" + taskname
                dashboardURL = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=" + username \
                             + "&refresh=0&table=Jobs&p=1&records=25&activemenu=2&status=&site=&tid=" + taskname
                self.logger.info("Glidemon monitoring URL:\t%s" % (glidemonURL))
                self.logger.info("Dashboard monitoring URL:\t%s" % (dashboardURL))

        #Print information about jobs
        states = dictresult['jobsPerStatus']
        if states:
            total = sum( states[st] for st in states )
            state_list = sorted(states)
            self.logger.info("Details:\t\t\t{0} {1}".format(self._printState(state_list[0], 13), self._percentageString(state_list[0], states[state_list[0]], total)))
            for status in state_list[1:]:
                self.logger.info("\t\t\t\t{0} {1}".format(self._printState(status, 13), self._percentageString(status, states[status], total)))


    def printErrors(self, dictresult):
        """ Iterate over dictresult['jobs'] if present, which is a dictionary like:
                {'10': {'State': 'running'}, '1': {'State': 'failed', 'Error' : [10,'error message']}, '3': {'State': 'failed', 'Error' : [10,'error message']} ...
            and group the errors per exit code counting how many jobs exited with a certain exit code, and print the summary
        """
        ec_numjobs = {} #a map containing the exit code as key and the number of jobs with this exit code as value
        ec_error = {}   #a map containint the exit code as key and the exit message as value (the last one we found)
        unknown = 0
        are_failed_jobs = False
        for jobid, status in dictresult['jobs'].iteritems():
            if status['State'] == 'failed':
                are_failed_jobs = True
                if 'Error' in status:
                    ec = status['Error'][0] #exit code of this failed job
                    ec_numjobs[ec] = ec_numjobs.setdefault(ec, 0) + 1
                    ec_error[ec] = status['Error'][1]
                else:
                    unknown += 1
        if are_failed_jobs:
            msg = "\nError summary:"
            for ec, count in ec_numjobs.iteritems():
                msg += "\n%s jobs failed with exit code %s:" % (count, ec)
                msg += '\n\t'+'\n\t'.join(ec_error[ec].split('\n'))
            if unknown:
                msg += "\nCould not find exit code details for %s jobs" % (unknown)
            msg += "\nHave a look at https://twiki.cern.ch/twiki/bin/viewauth/CMSPublic/JobExitCodes for a description of the exit codes"
            self.logger.info(msg)


    def printPublication(self, dictresult):
        """
        Print information about the publication of the output files in DBS.
        """
        ## If publication was disabled, print a pertinent message and return.
        if 'disabled' in dictresult['publication']:
            msg = "\nNo publication information (publication has been disabled in the CRAB configuration file)"
            self.logger.info(msg)
            return
        ## List of output datasets that are going to be (or are already) published. This
        ## list is written into the Tasks DB by the post-job when it does the upload of
        ## the output files metadata. This means that the list will be empty until one
        ## of the post-jobs will finish executing.
        outputDatasets = dictresult.get('outdatasets')
        ## Function to print the list of output datasets (with or without the DAS URL).
        def printOutputDatasets(includeDASURL = False):
            if outputDatasets:
                msg = ""
                if includeDASURL:
                    for outputDataset in outputDatasets:
                        msg += "\nOutput dataset:\t\t\t%s" % (outputDataset)
                       	msg += "\nOutput dataset DAS URL:\t\thttps://cmsweb.cern.ch/das/request?input={0}&instance=prod%2Fphys03".format(urllib.quote(outputDataset, ''))
                else:
                    plural = "s" if len(outputDatasets) > 1 else ""
                    extratab = "\t" if not plural else ""
                    msg += "\nOutput dataset%s:\t\t%s%s" % (plural, extratab, outputDatasets[0])
                    for outputDataset in outputDatasets[1:]:
                        msg += "\n\t\t\t\t%s%s" % (extratab, outputDataset)
                self.logger.info(msg)
        ## If publication information is not available yet, print a pertinent message
        ## (print first the list of output datasets, without the DAS URL) and return.
        if not dictresult['publication']:
            printOutputDatasets()
            msg = "\nNo publication information available yet"
            self.logger.info(msg)
            return
        ## Case in which there was an error in retrieving the publication status.
        if 'error' in dictresult['publication']:
            msg = "\nPublication status:\t\t%s" % (dictresult['publication']['error'])
            self.logger.info(msg)
        elif dictresult['publication'] and outputDatasets:
            states = dictresult['publication']
            ## Don't consider publication states for which 0 files are in this state.
            states_tmp = states.copy()
            for status in states:
                if states[status] == 0:
                    del states_tmp[status]
            states = states_tmp.copy()
            ## Count the total number of files to publish. For this we count the number of
            ## jobs and the number of files to publish per job (which is equal to the number
            ## of output datasets produced by the task, because, when multiple EDM files are
            ## produced, each EDM file goes into a different output dataset).
            numJobs = sum(dictresult['jobsPerStatus'].values())
            numOutputDatasets = len(outputDatasets)
            numFilesToPublish = numJobs * numOutputDatasets
            ## Count how many of these files have already started the publication process.
            numSubmittedFiles = sum(states.values())
            ## Substract the above two numbers to obtain how many files have not yet been
            ## considered for publication.
            states['unsubmitted'] = numFilesToPublish - numSubmittedFiles
            ## Print the publication status.
            statesList = sorted(states)
            msg = "\nPublication status:\t\t{0} {1}".format(self._printState(statesList[0], 13), \
                                                            self._percentageString(statesList[0], states[statesList[0]], numFilesToPublish))
            for status in statesList[1:]:
                if states[status]:
                    msg += "\n\t\t\t\t{0} {1}".format(self._printState(status, 13), \
                                                      self._percentageString(status, states[status], numFilesToPublish))
            self.logger.info(msg)
        ## Print the output datasets with the corresponding DAS URL.
        printOutputDatasets(includeDASURL = True)


    def printSummary(self, dictresult):

        sites = {}
        default_info = {"Runtime": 0, "Waste": 0, "Running": 0, "Success": 0, "Failed": 0, "Stageout": 0}
        for i in range(1, len(dictresult['jobs'])+1):
            info = dictresult['jobs'][str(i)]
            state = info['State']
            site_history = info.get("SiteHistory")
            if not site_history:
                continue
            walls = info['WallDurations']
            cur_site = info['SiteHistory'][-1]
            cur_info = sites.setdefault(cur_site, dict(default_info))
            for site, wall in zip(site_history[:-1], walls[:-1]):
                info = sites.setdefault(site, dict(default_info))
                info['Failed'] += 1
                info['Waste'] += wall
            if state in ['failed', 'cooloff', 'held', 'killed'] or (state == 'idle' and cur_site != 'Unknown'):
                cur_info['Failed'] += 1
                cur_info['Waste'] += walls[-1]
            elif state == 'transferring':
                cur_info['Stageout'] += 1
                cur_info['Runtime'] += walls[-1]
            elif state == 'running':
                cur_info['Running'] += 1
                cur_info['Runtime'] += walls[-1]
            elif state == 'finished':
                cur_info['Success'] += 1
                cur_info['Runtime'] += walls[-1]

        self.logger.info("\nSite Summary Table (including retries)\n")
        self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % ("Site", "Runtime", "Waste", "Running", "Successful", "Stageout", "Failed"))

        sorted_sites = sites.keys()
        sorted_sites.sort()
        for site in sorted_sites:
            info = sites[site]
            if site == 'Unknown': continue
            self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % (site, to_hms(info['Runtime']), to_hms(info['Waste']), str(info['Running']), str(info['Success']), str(info['Stageout']), str(info['Failed'])))

        self.logger.info("")


    def printLong(self, dictresult):
        sortdict = {}
        self.logger.info("\nExtended Job Status Table\n")
        self.logger.info("%4s %-12s %-20s %10s %10s %10s %10s %10s %10s %10s" % ("Job", "State", "Most Recent Site", "Runtime", "Mem (MB)", "CPU %", "Retries", "Restarts", "Waste", "Exit Code"))
        mem_cnt = 0
        mem_min = -1
        mem_max = 0
        mem_sum = 0
        run_cnt = 0
        run_min = -1
        run_max = 0
        run_sum = 0
        cpu_min = -1
        cpu_max = 0
        cpu_sum = 0
        wall_sum = 0
        for i in range(1, len(dictresult['jobs'])+1):
            info = dictresult['jobs'][str(i)]
            state = info['State']
            site = ''
            if info.get('SiteHistory'): site = info['SiteHistory'][-1]
            wall = 0
            if info.get('WallDurations'):
                wall = info['WallDurations'][-1]
                run_cnt += 1
                if (run_min == -1) or (wall < run_min): run_min = wall
                if wall > run_max: run_max = wall
            run_sum += wall
            wall_str = to_hms(wall)
            waste = 0
            if info.get('WallDurations'):
                for run in info['WallDurations'][:-1]:
                    waste += run
            wall_sum += waste + wall
            waste = to_hms(waste)
            mem = 'Unknown'
            if info.get('ResidentSetSize'):
                mem = info['ResidentSetSize'][-1]/1024
                mem_cnt += 1
                if (mem_min == -1) or (wall < mem_min): mem_min = mem
                if mem > mem_max: mem_max = mem
                mem_sum += mem
                mem = '%d' % mem
            cpu = 'Unknown'
            if (state in ['cooloff', 'failed', 'finished']) and not wall:
                cpu = 0
                if (cpu_min == -1) or cpu < cpu_min: cpu_min = cpu
                if cpu > cpu_max: cpu_max = cpu
                cpu = "%.0f" % cpu
            elif wall and ('TotalSysCpuTimeHistory' in info) and ('TotalUserCpuTimeHistory' in info):
                cpu = info['TotalSysCpuTimeHistory'][-1] + info['TotalUserCpuTimeHistory'][-1]
                cpu_sum += cpu
                if not wall: cpu = 0
                else: cpu = (cpu / float(wall)) * 100
                if (cpu_min == -1) or cpu < cpu_min: cpu_min = cpu
                if cpu > cpu_max: cpu_max = cpu
                cpu = "%.0f" % cpu
            ec = 'Unknown'
            if 'Error' in info:
                ec = str(info['Error'][0]) #exit code of this failed job
            elif state in ['finished']:
                ec = '0'
            sortdict[str(i)] = {'state' : state , 'site' : site , 'runtime' : wall_str , 'memory' : mem , 'cpu' : cpu , 'retries' : info.get('Retries', 0) , 'restarts' : info.get('Restarts', 0) , 'waste' : waste, 'exitcode' : ec}
            self.logger.info("%4d %-12s %-20s %10s %10s %10s %10s %10s %10s %10s" % (i, state, site, wall_str, mem, cpu, info.get('Retries', 0), info.get('Restarts', 0), waste, ec))
        self.logger.debug("")
        self.logger.debug("%4s %-10s" % ("Job", "Cluster Id"))
        for i in range(1, len(dictresult['jobs'])+1):
            info = dictresult['jobs'][str(i)]
            cluster = str(info.get('JobIds', 'Unknown'))
            self.logger.debug("%4d %10s" % (i, cluster))

        if hasattr(self, 'sort') and  self.sort != None:
            self.printSort(sortdict,self.sort)

        self.logger.info("\nSummary:")
        if mem_cnt:
            self.logger.info(" * Memory: %dMB min, %dMB max, %.0fMB ave" % (mem_min, mem_max, mem_sum/mem_cnt))
        if run_cnt:
            self.logger.info(" * Runtime: %s min, %s max, %s ave" % (to_hms(run_min), to_hms(run_max), to_hms(run_sum/run_cnt)))
        if run_sum and cpu_min >= 0:
            self.logger.info(" * CPU eff: %.0f%% min, %.0f%% max, %.0f%% ave" % (cpu_min, cpu_max, (cpu_sum / run_sum)*100))
        if wall_sum or run_sum:
            waste = wall_sum - run_sum
            self.logger.info(" * Waste: %s (%.0f%% of total)" % (to_hms(waste), (waste / float(wall_sum))*100))
        self.logger.info("")


    def printIdle(self, dictresult, task_user):
        sites = {}
        found_idle = False
        for i in range(1, len(dictresult['jobs'])+1):
            info = dictresult['jobs'][str(i)]
            state = info['State']
            if state != 'idle':
                continue
            found_idle = True
            job_site_list = info.get("AvailableSites", [])
            for orig_site in job_site_list:
                site = orig_site.replace("_Buffer", "").replace("_Disk", "").replace("_Tape", "")
                if site != orig_site and site in job_site_list and orig_site in job_site_list:
                    continue
                site_info = sites.setdefault(site, {'Pending': 0})
                site_info['Pending'] += 1
        if not found_idle:
            self.logger.info("\nNo idle jobs to analyze.\n")
            return

        if 'pool' in dictresult:
            for site in sites:
                if site not in dictresult['pool']:
                    continue
                site_info = dictresult['pool'][site]
                if 'IdleGlideins' in site_info:
                    sites[site]['IdleGlideins'] = site_info["IdleGlideins"]
                tot_prio = 0
                tot_resources = 0
                user_running = 0
                equal_prio = 0
                higher_prio = 0
                user_prio = 0
                for user, user_info in site_info.items():
                    if user == "IdleGlideins":
                        continue
                    if 'Priority' in user_info:
                        tot_prio += 1.0 / user_info['Priority']
                        if user == task_user:
                            user_prio = 1.0 / user_info['Priority']
                    tot_resources += user_info.get("Resources", 0)
                    if task_user != user:
                        continue
                    user_running += user_info.get("Resources", 0)
                    task_prio = 0
                    for task, task_info in user_info.get("tasks", {}).items():
                        if 'Priority' not in task_info:
                            continue
                        if task != self.cachedinfo['RequestName']:
                            continue
                        task_prio = task_info['Priority']
                        break
                    for task, task_info in user_info.get("tasks", {}).items():
                        if 'Priority' not in task_info:
                            continue
                        if task_info['Priority'] == task_prio:
                            equal_prio += 1
                        elif task_info['Priority'] > task_prio:
                            higher_prio += 1
                if not tot_prio:
                    continue
                sites[site]['UserShare'] = int(round(user_prio / float(tot_prio) * tot_resources))
                sites[site]['TotalShare'] = tot_resources
                sites[site]['CurUserShare'] = user_running
                sites[site]['HigherPrio'] = higher_prio
                sites[site]['EqualPrio'] = equal_prio

        self.logger.info("\nIdle Job Summary Table\n")
        self.logger.info("%-20s  %8s  %14s  %16s  %13s" % ("Possible Site", "Matching", "Your Current /", "Your Higher /", "Running /"))
        self.logger.info("%-20s  %8s  %14s  %16s  %13s" % ("", "Jobs", "Entitled Slots", "Equal Prio Tasks", "Queued Pilots"))
        not_in_pool = []
        max_share = 0
        max_tasks = 0
        max_glideins = 0
        for site in sorted(sites):
            site_info = sites[site]
            if site not in dictresult.get("pool", {}) or 'UserShare' not in site_info:
                continue
            max_share = max(site_info['CurUserShare'], site_info['UserShare'], max_share)
            max_tasks = max(site_info["HigherPrio"], site_info['EqualPrio'], max_tasks)
            max_glideins = max(site_info['TotalShare'], site_info.get("IdleGlideins", 0))
        share_digit_count = int(math.ceil(math.log(max_share+1, 10)))
        tasks_digit_count = int(math.ceil(math.log(max_tasks+1, 10)))
        glideins_digit_count = int(math.ceil(math.log(max_glideins+1, 10)))
        for site in sorted(sites):
            site_info = sites[site]
            if site not in dictresult.get("pool", {}) or 'UserShare' not in site_info:
                not_in_pool.append(site)
                continue
            pending_count = site_info['Pending']
            share = ("%" + str(share_digit_count) + "d / %" + str(share_digit_count) + "d") % (site_info['CurUserShare'], site_info['UserShare'])
            prio = ("%" + str(tasks_digit_count) + "d / %" + str(tasks_digit_count) + "d") % (site_info["HigherPrio"], site_info['EqualPrio'])
            glideins = ("%" + str(glideins_digit_count) + "d / %" + str(glideins_digit_count) + "s") % (site_info['TotalShare'], site_info.get("IdleGlideins", "?"))
            self.logger.info("%-20s  %8s  %14s  %16s  %13s" % (site, pending_count, share, prio, glideins))
        self.logger.info("")

        if not_in_pool:
            self.logger.info("The following sites could run jobs but currently have no running or pending pilots:\n")
            self.logger.info("%-20s %13s" % ("Possible Site", "Matching Jobs"))
            for site in sorted(not_in_pool):
                self.logger.info("%-20s %13s" % (site, sites[site]['Pending']))
            self.logger.info("")


    def printSort(self, sortdict, sortby):
        sortmatrix = []
        valuedict ={}
        self.logger.info('')
        for id in sortdict:
            if sortby in ['state' , 'site', 'exitcode']:
                value = sortdict[id][sortby]
                if not value in valuedict:
                    valuedict[value] = str(id)
                else:
                    valuedict[value] = valuedict[value]+','+str(id)
            elif sortby in ['memory', 'cpu' , 'retries']:
                if not sortdict[id][sortby] == 'Unknown':
                    value = int(sortdict[id][sortby])
                else:
                    value = 9999
                sortmatrix.append((value,id))
                sortmatrix.sort()
            elif sortby in ['runtime' ,'waste']:
                value = sortdict[id][sortby]
                realvaluematrix = value.split(':')
                realvalue = 3600*int(realvaluematrix[0]) + 60*int(realvaluematrix[1]) + int(realvaluematrix[2])
                sortmatrix.append((realvalue,value,id))
                sortmatrix.sort()

        if sortby in ['state' , 'site']:
            self.logger.info('Job sorted by %s: \n' % sortby)
            self.logger.info('%-20s %-20s\n' % (sortby.title(), 'Job Id(s)'))
            for value in valuedict:
                self.logger.info('%-20s %-s' % (value,valuedict[value]))
            self.logger.info('')
        elif sortby in ['memory', 'cpu', 'retries']:
            self.logger.info('Job sorted by %s used: \n' % sortby)
            if sortby == 'memory':
                self.logger.info('%-10s %-10s' % ('Memory (MB)'.center(10), 'Job Id'.center(10)))
            elif sortby == 'cpu':
                self.logger.info('%-10s %-10s' % ('CPU'.center(10), 'Job Id'.center(10)))
            elif sortby == 'retries':
                self.logger.info('%-10s %-10s' % ('Retries'.center(10), 'Job Id'.center(10)))
            for value in sortmatrix:
                if value[0] == 9999:
                    esignvalue = 'Unknown'
                else: esignvalue = value[0]
                self.logger.info('%10s %10s' %(str(esignvalue).center(10),value[1].center(10)))
            self.logger.info('')
        elif sortby in ['runtime' ,'waste']:
            self.logger.info('Job sorted by %s used: \n' % sortby)
            self.logger.info('%-10s %-5s\n' % (sortby.title(), 'Job Id'))
            for value in sortmatrix:
                self.logger.info('%-10s %-5s' % (value[1],value[2].center(5)))
            self.logger.info('')

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option( '--long',
                                dest = 'long',
                                action = 'store_true',
                                default = False,
                                help = 'Print one status line per running job.')
        self.parser.add_option( "--json",
                                dest = "json",
                                default = False,
                                action = "store_true",
                                help = "Print status results in JSON.")
        self.parser.add_option( "--summary",
                                dest = "summary",
                                default = False,
                                action = "store_true",
                                help = "Print site summary.")
        self.parser.add_option( "--idle",
                                dest = "idle",
                                default = False,
                                action = "store_true",
                                help = "Print idle job summary.")
        self.parser.add_option( "--sort",
                                dest = "sort",
                                default = None,
                                help = 'Only use with option long, availble sorting: "state", "site", "runtime", "memory", "cpu", "retries", "waste" and "exitcode"')

    def validateOptions(self):
        SubCommand.validateOptions(self)
        if self.options.idle and (self.options.long or self.options.summary):
            raise ConfigurationException("Idle option (-i) conflicts with -u, and -l")
        self.json = self.options.json
        self.summary = self.options.summary
        self.idle = self.options.idle
        self.long = self.options.long

        acceptedsort = ["state", "site", "runtime", "memory", "cpu", "retries", "waste", "exitcode"]
        if hasattr(self.options , 'sort') and self.options.sort != None:
            if not self.long:
                raise ConfigurationException('%sError%s: Please use option --long together with --sort' % (colors.RED, colors.NORMAL))
            elif not self.options.sort in acceptedsort:
                raise ConfigurationException('%sError%s: Only these values are accepted for crab status --sort option: %s' % (colors.RED, colors.NORMAL, acceptedsort))
            else:
                self.sort = self.options.sort
