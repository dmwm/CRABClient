from __future__ import division # I want floating points
from __future__ import print_function

import CRABClient.Emulator
import urllib
from CRABClient import __version__
import math
import ast

from CRABClient.UserUtilities import getFileFromURL
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ClientExceptions import ConfigurationException
from CRABClient.ClientUtilities import colors
from ServerUtilities import TASKDBSTATUSES_TMP
from ServerUtilities import getProxiedWebDir


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

    def __call__(self):
        taskname = self.cachedinfo['RequestName']
        uri = self.getUrl(self.instance, resource = 'task')
        webdir = getProxiedWebDir(taskname, self.serverurl, uri, self.proxyfilename, self.logger.debug)
        filename = 'status_cache'
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        server = serverFactory(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        if not webdir:
            dictresult, _, _ =  server.get(uri, data = {'subresource': 'webdir', 'workflow': taskname})
            webdir = dictresult['result'][0]
            self.logger.info('Server result: %s' % webdir)

        # Download status_cache file
        url = webdir + '/' + filename
        longStReportFile = open(getFileFromURL(url, proxyfilename=self.proxyfilename))

        # Skip first line of the file (it contains info for the caching script) and load job_report summary 
        longStReportFile.readline()
        self.jobLogNodeStateSummary = ast.literal_eval(longStReportFile.readline())
        # Load error_summary
#         self.errSummary = longStReportFile.readline()
        # Load node_state summary
#         self.node_state_summary = ast.literal_eval(longStReportFile.readline())

        DBInfo, _, _ = server.get(self.uri, data = {'workflow': self.cachedinfo['RequestName'], 'verbose': 0})
        DBInfo = DBInfo['result'][0]
        user = self.cachedinfo['RequestName'].split("_")[2].split(":")[-1]

        self.printTaskInfo(DBInfo, user)
        self.printShort(self.jobLogNodeStateSummary)
        if self.options.long:
            self.printLong(self.jobLogNodeStateSummary)

    def printTaskInfo(self, dictresult, username):
        """ Print general information like project directory, task name, scheduler, task status (in the database),
            dashboard URL, warnings and failire messages in the database.
        """
        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}

        self.logger.info("CRAB project directory:\t\t%s" % (self.requestarea))
        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        if dictresult['schedd']:
            msg = "Grid scheduler:\t\t\t%s" % (dictresult['schedd'])
            self.logger.info(msg)
        msg = "DB task status:\t\t\t"
        if 'FAILED' in dictresult['status']:
            msg += "%s%s%s" % (colors.RED, dictresult['status'], colors.NORMAL)
        else:
            if dictresult['status'] in TASKDBSTATUSES_TMP:
                msg += "%s on command %s" % (dictresult['status'], dictresult['command'])
            else:
                msg += "%s" % (dictresult['status'])
        self.logger.info(msg)

        # Get task status from the node_state/job_log summary
        dagman_codes = {1: 'SUBMITTED', 2: 'SUBMITTED', 3: 'SUBMITTED', 4: 'SUBMITTED', 5: 'COMPLETED', 6: 'FAILED'}
        task_status =  dagman_codes.get(self.jobLogNodeStateSummary['DagStatus']['DagStatus'])
        msg = "DAG status:\t\t\t" + task_status
        self.logger.info(msg)

        ## Show dashboard URL for the task.
        if dictresult['schedd']:
            ## Print the Dashboard monitoring URL for this task.
            taskname = urllib.quote(self.cachedinfo['RequestName'])
            dashboardURL = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=" + username \
                         + "&refresh=0&table=Jobs&p=1&records=25&activemenu=2&status=&site=&tid=" + taskname
            self.logger.info("Dashboard monitoring URL:\t%s" % (dashboardURL))

        ## Print the warning messages (these are the warnings in the Tasks DB,
        ## and/or maybe some warning added by the REST Interface to the status result).
        if dictresult['taskWarningMsg']:
            for warningMsg in dictresult['taskWarningMsg']:
                self.logger.warning("%sWarning%s:\t\t\t%s" % (colors.RED, colors.NORMAL, warningMsg))
        if dictresult['taskFailureMsg'] or dictresult['statusFailureMsg']:
            if dictresult['taskFailureMsg']:
                msg  = "%sFailure message%s:" % (colors.RED, colors.NORMAL)
                msg += "\t\t%s" % (dictresult['taskFailureMsg'].replace('\n', '\n\t\t\t\t'))
                self.logger.error(msg)
            if dictresult['statusFailureMsg']:
                msg = "%sError retrieving task status%s:" % (colors.RED, colors.NORMAL)
                msg += "\t%s" % (dictresult['statusFailureMsg'].replace('\n', '\n\t\t\t\t'))
                self.logger.error(msg)

    def printLong(self, dictresult, quiet = False):
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
        for jobid in dictresult.keys():
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

        ## Print (to the log file) a table with the HTCondor cluster id for each job.
        msg = "\n%4s %-10s" % ("Job", "Cluster Id")
        for jobid in dictresult.keys():
            info = dictresult[str(jobid)]
            clusterid = str(info.get('JobIds', 'Unknown'))
            msg += "\n%4s %10s" % (jobid, clusterid)
        if not quiet:
            self.logger.debug(msg)

        ## Print a summary with memory/cpu usage.
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

    def printShort(self, dictresult):
        """ Give a summary of the job statuses, keeping in mind that:
                - If there is a job with id 0 then this is the probe job for the estimation
                  This is the so called automatic splitting
                - Then you have normal jobs
                - Jobs that are line 1-1, 1-2 and so on are completing
        """

        if 'DagStatus' in dictresult:
            del dictresult['DagStatus']

#        taskJobCount = len(dictresult)
#
#         for i in range(1, taskJobCount+1):
#             i = str(i)
#             if i not in dictresult:
#                 if taskStatusCode == 5:
#                     taskStatus[i] = {'State': 'killed'}
#                 else:
#                     taskStatus[i] = {'State': 'unsubmitted'}

        jobsPerStatus = {}
        jobList = []
        result= {}
        for job, info in dictresult.items():
            job = int(job)
            status = info['State']
            jobsPerStatus.setdefault(status, 0)
            jobsPerStatus[status] += 1
            jobList.append((status, job))
        result['jobsPerStatus'] = jobsPerStatus
        result['jobList'] = jobList
#         result['jobs'] = dictresult

        ## Print information  about the single splitting job
        if '0' in dictresult:
            statusSplJob = dictresult['0']['State']
            self.logger.info("\nSplitting job status:\t\t{0}".format(self._printState(statusSplJob, 13)))

        ## Collect information about jobs
        # Create a dictionary like { 'finished' : 1, 'running' : 3}
        states = {}
        for jobid, statusDict in dictresult.iteritems():
            status = statusDict['State']
            if jobid == '0' or '-' in jobid:
                continue
            states[status] = states.setdefault(status, 0) + 1


        ## Collect information about subjobs
        # Create a dictionary like { 'finished' : 1, 'running' : 3}
        statesSJ = {}
        for jobid, statusDict in dictresult.iteritems():
            status = statusDict['State']
            if jobid == '0' or '-' not in jobid:
                continue
            statesSJ[status] = statesSJ.setdefault(status, 0) + 1

        # And if the dictionary is not empty, print it
        for jobtype, currStates in [('Jobs', states), ('Completing jobs', statesSJ)]:
            if currStates:
                total = sum( states[st] for st in states )
                state_list = sorted(states)
                self.logger.info("\n{0} status:\t\t\t{1} {2}".format(jobtype, self._printState(state_list[0], 13), self._percentageString(state_list[0], states[state_list[0]], total)))
                for status in state_list[1:]:
                    self.logger.info("\t\t\t\t{0} {1}".format(self._printState(status, 13), self._percentageString(status, states[status], total)))

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
        self.parser.add_option("--idle",
                               dest = "idle",
                               default = False,
                               action = "store_true",
                               help = "Print summary for idle jobs.")
        self.parser.add_option("--verboseErrors",
                               dest = "verboseErrors",
                               default = False,
                               action = "store_true",
                               help = "Expand error summary, showing error messages for all failed jobs.")


    def validateOptions(self):
        SubCommand.validateOptions(self)
        if self.options.idle and (self.options.long or self.options.summary):
            raise ConfigurationException("Option --idle conflicts with --summary and --long")

def to_hms(val):
    s = val % 60
    val -= s
    val = (val / 60)
    m = val % 60
    val -= m
    h = val / 60
    return "%d:%02d:%02d" % (h, m, s)

