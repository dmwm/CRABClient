from __future__ import division # I want floating points
from __future__ import print_function

import CRABClient.Emulator
import urllib
from CRABClient import __version__
import math
from ast import literal_eval
import json

from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientUtilities import colors
from ServerUtilities import TASKDBSTATUSES_TMP


PUBLICATION_STATES = {
    'not_published': 'idle',
    'publication_failed': 'failed',
    'published': 'finished',
    'publishing': 'running',
}

class status2(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by the -d/--dir option.
    """

    shortnames = ['st2']


    def getColumn(self, dictresult, columnName):
        columnIndex = dictresult['desc']['columns'].index(columnName)
        value = dictresult['result'][columnIndex]
        return value

    def __call__(self):
        # Get all of the columns from the database for a certain task
        taskname = self.cachedinfo['RequestName']
        uri = self.getUrl(self.instance, resource = 'task')
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)
        crabDBInfo, _, _ =  server.get(uri, data = {'subresource': 'search', 'workflow': taskname})
        self.logger.debug("Got information from server oracle database: %s", crabDBInfo)

        user = self.getColumn(crabDBInfo, 'tm_username')
        webdir = self.getColumn(crabDBInfo, 'tm_user_webdir')
        rootDagId = self.getColumn(crabDBInfo, 'clusterid') #that's the condor id from the TW

        #Print information from the database
        self.printTaskInfo(crabDBInfo, user)
        if rootDagId and not webdir:
            # if the dag is submitted and the webdir is not there we have to wait that AdjustSites run
            # and upload the webdir location to the server
            self.logger.info("The CRAB server submitted your task to the Grid scheduler (ID: %s)")
            self.logger.info("Waiting for the scheduler to report back the status of your task")
            return crabDBInfo, None

        self.logger.debug("Webdir is located at %s", webdir)
        # Download status_cache file
        self.logger.debug("Retrieving 'status_cache' file from webdir")
        url = webdir + '/' + "status_cache"

        statusCacheInfo = None
        statusCacheFilename = getFileFromURL(url, proxyfilename=self.proxyfilename)
        with open(statusCacheFilename) as fd:
            # Skip first line of the file (it contains info for the caching script) and load job_report summary
            fd.readline()
            statusCacheInfo = literal_eval(fd.readline())
        self.logger.debug("Got information from status cache file: %s", statusCacheInfo)

        self.printDAGStatus(statusCacheInfo)

        shortResult = self.printShort(statusCacheInfo)
        self.printErrors(statusCacheInfo)
        if self.options.summary:
            self.printSummary(statusCacheInfo)
        if self.options.long or self.options.sort:
            sortdict = self.printLong(statusCacheInfo, quiet = (not self.options.long))
            if self.options.sort:
                self.printSort(sortdict, self.options.sort)
        if self.options.json:
            self.logger.info(json.dumps(statusCacheInfo))

        return crabDBInfo, shortResult

    def _percentageString(self, state, value, total):
        state = PUBLICATION_STATES.get(state, state)
        digit_count = int(math.ceil(math.log(max(value, total)+1, 10)))
        format_str = "%5.1f%% (%s%" + str(digit_count) + "d%s/%" + str(digit_count) + "d)"
        return format_str % ((value*100/total), self._stateColor(state), value, colors.NORMAL, total)


    def _printState(self, state, ljust):
        state = PUBLICATION_STATES.get(state, state)
        return ('{0}{1:<' + str(ljust) + '}{2}').format(self._stateColor(state), state, colors.NORMAL)

    def _stateColor(self, state):
        if state == 'failed':
            return colors.RED
        elif state == 'running':
            return colors.GREEN
        elif state == 'idle' or state == 'unsubmitted':
            return colors.GRAY
        else:
            return colors.NORMAL

    def printDAGStatus(self, statusCacheInfo):
        # Get dag status from the node_state/job_log summary
        dagman_codes = {1:'SUBMITTED', 2:'SUBMITTED', 3:'SUBMITTED', 4:'SUBMITTED', 5:'COMPLETED', 6:'FAILED'}
        dag_status = dagman_codes.get(statusCacheInfo['DagStatus']['DagStatus'])
        msg = "Status on the scheduler:\t" + dag_status
        self.logger.info(msg)
        return msg

    def printTaskInfo(self, dictresult, username):
        """ Print general information like project directory, task name, scheduler, task status (in the database),
            dashboard URL, warnings and failire messages in the database.
        """
        schedd = self.getColumn(dictresult, 'tm_schedd')
        status = self.getColumn(dictresult, 'tm_task_status')
        command = self.getColumn(dictresult, 'tm_task_command')
        warnings = literal_eval(self.getColumn(dictresult, 'tm_task_warnings'))
        failure = literal_eval(self.getColumn(dictresult, 'tm_task_failure'))

        self.logger.info("CRAB project directory:\t\t%s" % (self.requestarea))
        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        if schedd:
            msg = "Grid scheduler:\t\t\t%s" % schedd
            self.logger.info(msg)
        msg = "Status on the CRAB server:\t"
        if 'FAILED' in status:
            msg += "%s%s%s" % (colors.RED, status, colors.NORMAL)
        else:
            if status in TASKDBSTATUSES_TMP:
                msg += "%s on command %s" % (status, command)
            else:
                msg += "%s" % (status)
        self.logger.info(msg)

        # Show dashboard URL for the task.
        if schedd:
            ## Print the Dashboard monitoring URL for this task.
            taskname = urllib.quote(self.cachedinfo['RequestName'])
            dashboardURL = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=" + username \
                         + "&refresh=0&table=Jobs&p=1&records=25&activemenu=2&status=&site=&tid=" + taskname
            self.logger.info("Dashboard monitoring URL:\t%s" % (dashboardURL))

        # Print the warning messages (these are the warnings in the Tasks DB,
        # and/or maybe some warning added by the REST Interface to the status result).
        if warnings:
            for warningMsg in warnings:
                self.logger.warning("%sWarning%s:\t\t\t%s" % (colors.RED, colors.NORMAL, warningMsg))
        if failure:
            msg  = "%sFailure message from the server%s:" % (colors.RED, colors.NORMAL)
            msg += "\t\t%s" % (failure.replace('\n', '\n\t\t\t\t'))
            self.logger.error(msg)

    def printLong(self, dictresult, quiet = False):
        """ Print detailed information about a task and each job.
        """
        sortdict = {}
        outputMsg  = "\nExtended Job Status Table:\n"
        outputMsg += "\n%4s %-12s %-20s %10s %10s %10s %10s %10s %10s %15s" \
                   % ("Job", "State", "Most Recent Site", "Runtime", "Mem (MB)", "CPU %", "Retries", "Restarts", "Waste", "Exit Code")
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
        def compareFunction(j1 ,j2):
            # j's can be '1' or '1-1'.
            x1 = map(int, j1.split('-'))
            x2 = map(int, j2.split('-'))
            return cmp(x1, x2) #using list comparison

        for jobid in sorted(dictresult.keys(), cmp=compareFunction):
            info = dictresult[str(jobid)]
            state = info['State']
            site = ''
            if info.get('SiteHistory'):
                site = info['SiteHistory'][-1]
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
            sortdict[str(jobid)] = {'state': state, 'site': site, 'runtime': wall_str, 'memory': mem, 'cpu': cpu, \
                                    'retries': info.get('Retries', 0), 'restarts': info.get('Restarts', 0), 'waste': waste, 'exitcode': ec}
            outputMsg += "\n%4s %-12s %-20s %10s %10s %10s %10s %10s %10s %15s" \
                       % (jobid, state, site, wall_str, mem, cpu, info.get('Retries', 0), info.get('Restarts', 0), waste, ' Postprocessing failed' if ec == '90000' else ec)
        if not quiet:
            self.logger.info(outputMsg)

        # Print (to the log file) a table with the HTCondor cluster id for each job.
        msg = "\n%4s %-10s" % ("Job", "Cluster Id")
        for jobid in dictresult.keys():
            info = dictresult[str(jobid)]
            clusterid = str(info.get('JobIds', 'Unknown'))
            msg += "\n%4s %10s" % (jobid, clusterid)
        if not quiet:
            self.logger.debug(msg)

        # Print a summary with memory/cpu usage.
        summaryMsg = "\nSummary:"
        if mem_cnt:
            summaryMsg += "\n * Memory: %dMB min, %dMB max, %.0fMB ave" % (mem_min, mem_max, mem_sum/mem_cnt)
        if run_cnt:
            summaryMsg += "\n * Runtime: %s min, %s max, %s ave" % (to_hms(run_min), to_hms(run_max), to_hms(run_sum/run_cnt))
        if run_sum and cpu_min >= 0:
            summaryMsg += "\n * CPU eff: %.0f%% min, %.0f%% max, %.0f%% ave" % (cpu_min, cpu_max, (cpu_sum / run_sum)*100)
        if wall_sum or run_sum:
            waste = wall_sum - run_sum
            summaryMsg += "\n * Waste: %s (%.0f%% of total)" % (to_hms(waste), (waste / float(wall_sum))*100)
        summaryMsg += "\n"
        if not quiet:
            self.logger.info(summaryMsg)

        return sortdict

    def printShort(self, statusCacheInfo):
        """ Give a summary of the job statuses, keeping in mind that:
                - If there is a job with id 0 then this is the probe job for the estimation
                  This is the so called automatic splitting
                - Then you have normal jobs
                - Jobs that are line 1-1, 1-2 and so on are completing
        """

        # This record is no longer necessary and makes parsing more difficult.
        if 'DagStatus' in statusCacheInfo:
            del statusCacheInfo['DagStatus']

        jobsPerStatus = {}
        jobList = []
        result= {}
        for job, info in statusCacheInfo.items():
            status = info['State']
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))
        result['jobsPerStatus'] = jobsPerStatus
        result['jobList'] = jobList

        # Print information  about the single splitting job
        if '0' in statusCacheInfo:
            statusSplJob = statusCacheInfo['0']['State']
            self.logger.info("\nSplitting job status:\t\t{0}".format(self._printState(statusSplJob, 13)))

        # Collect information about jobs
        # Create a dictionary like { 'finished' : 1, 'running' : 3}
        states = {}
        for jobid, statusDict in statusCacheInfo.iteritems():
            status = statusDict['State']
            if jobid == '0' or '-' in jobid: #skip splitting and completing jobs
                continue
            states[status] = states.setdefault(status, 0) + 1


        # Collect information about subjobs
        # Create a dictionary like { 'finished' : 1, 'running' : 3}
        statesSJ = {}
        for jobid, statusDict in statusCacheInfo.iteritems():
            status = statusDict['State']
            if jobid == '0' or '-' not in jobid: #skip splitting and normal jobs
                continue
            statesSJ[status] = statesSJ.setdefault(status, 0) + 1

        # And if the dictionary is not empty, print it
        for jobtype, currStates in [('Jobs', states), ('Completing jobs', statesSJ)]:
            if currStates:
                total = sum( currStates[st] for st in currStates )
                state_list = sorted(currStates)
                self.logger.info("\n{0:32}{1} {2}".format(jobtype + ' status:', self._printState(state_list[0], 13), self._percentageString(state_list[0], currStates[state_list[0]], total)))
                for status in state_list[1:]:
                    self.logger.info("\t\t\t\t{0} {1}".format(self._printState(status, 13), self._percentageString(status, currStates[status], total)))
        return result

    def printErrors(self, dictresult):
        """ Iterate over dictresult['jobs'] if present, which is a dictionary like:
                {'10': {'State': 'running'}, '1': {'State': 'failed', 'Error' : [10,'error message']}, '3': {'State': 'failed', 'Error' : [10,'error message']} ...
            and group the errors per exit code counting how many jobs exited with a certain exit code, and print the summary
        """
        ## In general, there are N_{ec}_{em} jobs that failed with exit code ec and
        ## error message em.
        ## Since there may be different error messages for a given exit code, we use a
        ## map containing the exit code as key and as value a list with the different
        ## exit messages. For example:
        ## ec_errors = { '1040' : ["Failed due to bla", "Failed due to blabla"],
        ##              '60302' : ["File X does not exist", "File Y does not exist"]
        ##             }
        ec_errors = {}
        ## We also want a map containing again the exit code as key, but as value a list
        ## with the lists of job ids that failed with given exit code and error message.
        ## For example:
        ## ec_jobids = { '1040' : [[1, 2, 3, 4, 5], [10, 12, 14]],
        ##              '60302' : [[6, 7, 8, 9, 10], [11, 13, 15]]
        ##             }
        ## where jobs [1, 2, 3, 4, 5] failed with error message "Failed due to bla",
        ## jobs [10, 12, 14] with "Failed due to blabla", jobs [6, 7, 8, 9, 10] with
        ## "File X does not exist" and jobs [11, 13, 15] with "File Y does not exist".
        ec_jobids = {}
        ## We also want a map containing again the exit code as key, but as value a list
        ## with the numbers N_{ec}_{em}. For example:
        ## ec_numjobs = { '1040' : [N_{1040}_{"Failed due to bla"}, N_{1040}_{"Failed due to blabla"}],
        ##               '60302' : [N_{60302}_{"File X does not exist"}, N_{60302}_{"File Y does not exist"}]
        ##              }
        ## Actually, for later convenience when sorting by most frequent error messages
        ## for each exit code (i.e. sorting by N_{ec}_{em}), we add a second number
        ## representing the position of the error message em in the corresponding list
        ## in ec_errors[ec]. For example:
        ## ec_numjobs = { '1040' : [(N_{1040}_{"Failed due to bla"}, 1), (N_{1040}_{"Failed due to blabla"}, 2)],
        ##               '60302' : [(N_{60302}_{"File X does not exist"}, 1), (N_{60302}_{"File Y does not exist"}, 2)]
        ##              }
        ## We will later sort ec_numjobs[ec], which sorts according to the first number
        ## in the 2-tuples. So the sorted ec_numjobs dictionary could be for example:
        ## ec_numjobs = { '1040' : [(N_{1040}_{"Failed due to bla"}, 1), (N_{1040}_{"Failed due to blabla"}, 2)],
        ##               '60302' : [(N_{60302}_{"File Y does not exist"}, 2), (N_{60302}_{"File X does not exist"}, 1)]
        ##              }
        ## The second number in the 2-tuples allows to get the error message from
        ## ec_errors[ec]: given the sorted ec_numjobs dictionary, for each exit code
        ## (i.e. for each key) ec in the dictionary, the most frequent error messages
        ## are ec_errors[ec][ec_numjobs[ec][0][1]], ec_errors[ec][ec_numjobs[ec][1][1]], etc.
        ec_numjobs = {}
        unknown = 0
        are_failed_jobs = False
        for jobid, status in dictresult.iteritems():
            if status['State'] == 'failed':
                are_failed_jobs = True
                if 'Error' in status:
                    ec = status['Error'][0] #exit code of this failed job
                    em = status['Error'][1] #exit message of this failed job
                    if ec not in ec_errors:
                        ec_errors[ec] = []
                    if ec not in ec_jobids:
                        ec_jobids[ec] = []
                    if ec not in ec_numjobs:
                        ec_numjobs[ec] = []
                    if em not in ec_errors[ec]:
                        ec_numjobs[ec].append((1, len(ec_errors[ec])))
                        ec_errors[ec].append(em)
                        ec_jobids[ec].append([jobid])
                    else:
                        i = ec_errors[ec].index(em)
                        ec_jobids[ec][i].append(jobid)
                        ec_numjobs[ec][i] = (ec_numjobs[ec][i][0] + 1, ec_numjobs[ec][i][1])
                else:
                    unknown += 1
        if are_failed_jobs:
            ## If option --sort=exitcodes was specified, show the error summary with the
            ## exit codes sorted. Otherwise show it sorted from most frequent exit code to
            ## less frequent.
            exitCodes = []
            if self.options.sort == "exitcode":
                for ec in ec_numjobs.keys():
                    if ec not in exitCodes:
                        exitCodes.append(ec)
                exitCodes.sort()
            else:
                numjobsec = []
                for ec, numjobs in ec_numjobs.iteritems():
                    count = 0
                    for nj, _ in numjobs:
                        count += nj
                    numjobsec.append((count, ec))
                numjobsec.sort()
                numjobsec.reverse()
                for _, ec in numjobsec:
                    if ec not in exitCodes:
                        exitCodes.append(ec)
            ## Sort the job ids in ec_jobids. Remember that ec_jobids[ec] is a list of lists
            ## of job ids, and that each job id is a string.
            for ec in ec_jobids.keys():
                for i in range(len(ec_jobids[ec])):
                    ec_jobids[ec][i] = [str(y) for y in sorted([int(x) for x in ec_jobids[ec][i]])]
            ## Error summary header.
            msg = "\nError Summary:"
            if not self.options.verboseErrors:
                msg += " (use crab status --verboseErrors for details about the errors)"
            ## Auxiliary variable for the layout of the error summary messages.
            totnumjobs = len(dictresult)
            ndigits = int(math.ceil(math.log(totnumjobs+1, 10)))
            ## For each exit code:
            for i, ec in enumerate(exitCodes):
                ## Sort the error messages for this exit code from most frequent one to less
                ## frequent one.
                numjobs = sorted(ec_numjobs[ec])
                numjobs.reverse()
                ## Count the total number of failed jobs with this exit code.
                count = 0
                for nj, _ in numjobs:
                    count += nj
                ## Exit code 90000 means failure in postprocessing stage.
                if ec == 90000:
                    msg += ("\n\n%" + str(ndigits) + "s jobs failed in postprocessing step%s") \
                         % (count, ":" if self.options.verboseErrors else "")
                else:
                    msg += ("\n\n%" + str(ndigits) + "s jobs failed with exit code %s%s") \
                         % (count, ec, ":" if self.options.verboseErrors else "")
                if self.options.verboseErrors:
                    ## Costumize the message depending on whether there is only one error message or
                    ## more than one.
                    if len(ec_errors[ec]) == 1:
                        error_msg = ec_errors[ec][0]
                        msg += ("\n\n\t%" + str(ndigits) + "s jobs failed with following error message:") % (nj)
                        msg += " (for example, job %s)" % (ec_jobids[ec][0][0])
                        msg += "\n\n\t\t" + "\n\t\t".join([line for line in error_msg.split('\n') if line])
                    else:
                        ## Show up to three different error messages.
                        remainder = count
                        if len(ec_errors[ec]) > 3:
                            msg += "\n\t(Showing only the 3 most frequent errors messages for this exit code)"
                        for nj, i in numjobs[:3]:
                            error_msg = ec_errors[ec][i]
                            msg += ("\n\n\t%" + str(ndigits) + "s jobs failed with following error message:") % (nj)
                            msg += " (for example, job %s)" % (ec_jobids[ec][i][0])
                            msg += "\n\n\t\t" + "\n\t\t".join([line for line in error_msg.split('\n') if line])
                            remainder -= nj
                        if remainder > 0:
                            msg += "\n\n\tFor the error messages of the other %s jobs," % (remainder)
                            msg += " please have a look at the dashboard task monitoring web page."
            if unknown:
                msg += "\n\nCould not find exit code details for %s jobs." % (unknown)
            msg += "\n\nHave a look at https://twiki.cern.ch/twiki/bin/viewauth/CMSPublic/JobExitCodes for a description of the exit codes."
            self.logger.info(msg)

    def printSummary(self, dictresult):
        """ Print the information about jobs on each site:
                - How many jobs are or were running on the site and in which state,
                - Runtime for each site.
        """

        sites = {}
        default_info = {"Runtime": 0, "Waste": 0, "Running": 0, "Success": 0, "Failed": 0, "Stageout": 0}
        for i in range(1, len(dictresult)+1):
            info = dictresult[str(i)]
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

        self.logger.info("\nSite Summary Table (including retries):\n")
        self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % ("Site", "Runtime", "Waste", "Running", "Successful", "Stageout", "Failed"))

        sorted_sites = sorted(sites.keys())
        for site in sorted_sites:
            info = sites[site]
            if site == 'Unknown': continue
            self.logger.info("%-20s %10s %10s %10s %10s %10s %10s" % (site, to_hms(info['Runtime']), to_hms(info['Waste']), str(info['Running']), str(info['Success']), str(info['Stageout']), str(info['Failed'])))

        self.logger.info("")

    def printSort(self, sortdict, sortby):
        """ Print information about jobs sorted by a certain attribute.
        """
        sortmatrix = []
        valuedict = {}
        self.logger.info('')
        for jobid in sortdict:
            if sortby in ['exitcode']:
                if sortdict[jobid][sortby] != 'Unknown':
                    value = int(sortdict[jobid][sortby])
                else:
                    value = 999999
                if value not in sortmatrix:
                    sortmatrix.append(value)
                sortmatrix.sort()
                if value not in valuedict:
                    valuedict[value] = [jobid]
                else:
                    valuedict[value].append(jobid)
            elif sortby in ['state' , 'site']:
                value = sortdict[jobid][sortby]
                if value not in valuedict:
                    valuedict[value] = [jobid]
                else:
                    valuedict[value].append(jobid)
            elif sortby in ['memory', 'cpu', 'retries']:
                if sortdict[jobid][sortby] != 'Unknown':
                    value = int(sortdict[jobid][sortby])
                else:
                    value = 999999
                sortmatrix.append((value, jobid))
                sortmatrix.sort()
            elif sortby in ['runtime', 'waste']:
                value = sortdict[jobid][sortby]
                realvaluematrix = value.split(':')
                realvalue = 3600*int(realvaluematrix[0]) + 60*int(realvaluematrix[1]) + int(realvaluematrix[2])
                sortmatrix.append((realvalue, value, jobid))
                sortmatrix.sort()
        if sortby in ['exitcode']:
            msg  = "Jobs sorted by exit code:\n"
            msg += "\n%-20s %-20s\n" % ('Exit Code', 'Job Id(s)')
            for value in sortmatrix:
                if value == 999999:
                    esignvalue = 'Unknown'
                else:
                    esignvalue = str(value)
                jobids = [str(jobid) for jobid in sorted([int(jobid) for jobid in valuedict[value]])]
                msg += "\n%-20s %-s" % (esignvalue, ", ".join(jobids))
            self.logger.info(msg)
        elif sortby in ['state' , 'site']:
            msg  = "Jobs sorted by %s:\n" % (sortby)
            msg += "\n%-20s %-20s\n" % (sortby.title(), 'Job Id(s)')
            for value in valuedict:
                msg += "\n%-20s %-s" % (value, ", ".join(valuedict[value]))
            self.logger.info(msg)
        elif sortby in ['memory', 'cpu', 'retries']:
            msg = "Jobs sorted by %s used:\n" % (sortby)
            if sortby == 'memory':
                msg += "%-10s %-10s" % ("Memory (MB)".center(10), "Job Id".center(10))
            elif sortby == 'cpu':
                msg += "%-10s %-10s" % ("CPU".center(10), "Job Id".center(10))
            elif sortby == 'retries':
                msg += "%-10s %-10s" % ("Retries".center(10), "Job Id".center(10))
            for value in sortmatrix:
                if value[0] == 999999:
                    esignvalue = 'Unknown'
                else:
                    esignvalue = value[0]
                msg += "%10s %10s" % (str(esignvalue).center(10), value[1].center(10))
            self.logger.info(msg)
        elif sortby in ['runtime' ,'waste']:
            msg  = "Jobs sorted by %s used:\n" % (sortby)
            msg += "%-10s %-5s\n" % (sortby.title(), "Job Id")
            for value in sortmatrix:
                msg += "%-10s %-5s" % (value[1], value[2].center(5))
            self.logger.info(msg)

        self.logger.info('')


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """

        self.parser.add_option("--long",
                               dest = "long",
                               default = False,
                               action = "store_true",
                               help = "Print one status line per job.")
        self.parser.add_option("--sort",
                               dest = "sort",
                               default = None,
                               help = "Sort failed jobs by 'state', 'site', 'runtime', 'memory', 'cpu', 'retries', 'waste' or 'exitcode'.")
        self.parser.add_option("--json",
                               dest = "json",
                               default = False,
                               action = "store_true",
                               help = "Print status results in JSON format.")
        self.parser.add_option("--summary",
                               dest = "summary",
                               default = False,
                               action = "store_true",
                               help = "Print site summary.")
        self.parser.add_option("--verboseErrors",
                               dest = "verboseErrors",
                               default = False,
                               action = "store_true",
                               help = "Expand error summary, showing error messages for all failed jobs.")


    def validateOptions(self):
        SubCommand.validateOptions(self)

def to_hms(val):
    s = val % 60
    val -= s
    val = (val / 60)
    m = val % 60
    val -= m
    h = val / 60
    return "%d:%02d:%02d" % (h, m, s)

