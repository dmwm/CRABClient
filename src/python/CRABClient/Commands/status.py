from __future__ import division # I want floating points
import urllib
import sys

from CRABClient.client_utilities import colors, getUserName
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.client_exceptions import MissingOptionException, RESTCommunicationException
from CRABClient import __version__
from RESTInteractions import HTTPRequests

def to_hms(val):
    s = val % 60
    val -= s
    val = (val / 60)
    m = val % 60
    val -= m
    h = val / 60
    return "%d:%02d:%02d" % (h, m, s)

class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    shortnames = ['st']
    states = ['submitted', 'failure', 'queued', 'success']

    def _percentageString(self, value, total):
        return "%.2f %% %s(%s/%s)%s" % ((value*100/total), colors.GRAY, value, total, colors.NORMAL)

    def __call__(self):
        server = HTTPRequests(self.serverurl, self.proxyfilename, self.proxyfilename, version=__version__)

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri, data = { 'workflow' : self.cachedinfo['RequestName'], 'verbose': int(self.summary or self.long or self.json)})
        dictresult = dictresult['result'][0] #take just the significant part

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            raise RESTCommunicationException(msg)

        self.printShort(dictresult)

        if 'jobs' not in dictresult:
            self.logger.info("\nNo jobs created yet!")
        else:
            if self.summary:
                self.printSummary(dictresult)
            if self.long:
               self.printLong(dictresult)
            if self.json:
               self.logger.info(dictresult['jobs'])

    def printShort(self, dictresult):

        self.logger.debug(dictresult) #should be something like {u'result': [[123, u'ciao'], [456, u'ciao']]}

        self.logger.info("Task name:\t\t\t%s" % self.cachedinfo['RequestName'])
        self.logger.info("Task status:\t\t\t%s" % dictresult['status'])

        def logJDefErr(jdef):
            """Printing job def failures if any"""
            if jdef['jobdefErrors']:
                self.logger.error("%sFailed to inject %s\t%s out of %s:" %(colors.RED, colors.NORMAL,\
                                                                           jdef['failedJobdefs'], jdef['totalJobdefs']))
                for error in jdef['jobdefErrors']:
                    self.logger.info("\t%s" % error)

        #Print the url of the panda monitor
        if dictresult['taskFailureMsg']:
            self.logger.error("%sError during task injection:%s\t%s" % (colors.RED,colors.NORMAL,dictresult['taskFailureMsg']))
            # We might also have more information in the job def errors
            logJDefErr(jdef=dictresult)
        elif self.cachedinfo['RequestName'] == dictresult['jobSetID']:
            # CRAB3-HTCondor
            taskname = urllib.quote(dictresult['jobSetID'])
            self.logger.info("Monitoring URL:\t\t\thttp://glidemon.web.cern.ch/glidemon/jobs.php?taskname=%s" % taskname)
        elif dictresult['jobSetID']:
            username = urllib.quote(getUserName(self.voRole, self.voGroup, self.logger))
            self.logger.info("Panda url:\t\t\thttp://pandamon-cms-dev.cern.ch/jobinfo?jobtype=*&jobsetID=%s&prodUserName=%s" % (dictresult['jobSetID'], username))
            # We have cases where the job def errors are there but we have a job def id
            logJDefErr(jdef=dictresult)

        #Print information about jobs
        states = dictresult['jobsPerStatus']
        total = sum( states[st] for st in states )
        frmt = ''
        for status in states:
            frmt += status + ' %s\t' % self._percentageString(states[status], total)
        if frmt:
            self.logger.info('Details:\t\t\t%s' % frmt)

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

        self.logger.info("\nExtended Job Status Table\n")
        self.logger.info("%4s %-12s %-20s %10s %10s %10s %10s %10s" % ("Job", "State", "Most Recent Site", "Runtime", "Mem (MB)", "CPU %", "Retries", "Waste"))
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
            if (state in ['cooloff', 'failed', 'finished']) and ('TotalSysCpuTimeHistory' in info) and info['TotalSysCpuTimeHistory']:
                cpu = info['TotalSysCpuTimeHistory'][-1] + info['TotalUserCpuTimeHistory'][-1]
                cpu_sum += cpu
                if not wall: cpu = 0
                else: cpu = (cpu / float(wall)) * 100
                if (cpu_min == -1) or cpu < cpu_min: cpu_min = cpu
                if cpu > cpu_max: cpu_max = cpu
                cpu = "%.0f" % cpu
            self.logger.info("%4d %-12s %-20s %10s %10s %10s %10s %10s" % (i, state, site, wall_str, mem, cpu, info.get('Retries', 0) + info.get('Restarts', 0), waste))

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

    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( '-l', '--long',
                                dest = 'long',
                                default = False,
                                action = "store_true",
                                help = 'Print one status line per running job')
        self.parser.add_option( "-j", "--json",
                                dest = "json",
                                default = False,
                                action = "store_true",
                                help = "Print status results in JSON")
        self.parser.add_option( "-u", "--summary",
                                dest = "summary",
                                default = False,
                                action = "store_true",
                                help = "Print site summary")
    
    def validateOptions(self):
        SubCommand.validateOptions(self)
        
        self.long = self.options.long
        self.json = self.options.json
        self.summary = self.options.summary

