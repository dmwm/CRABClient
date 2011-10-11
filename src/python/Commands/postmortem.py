from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests
from client_utilities import loadCache, getWorkArea
import os
import hashlib

class postmortem(SubCommand):
    """ Retrieve post mortem infromation of all the jobs in the task, or detailed
    information of just one job. The is identified by -t/--task option
    """

    name  = "get-errors"
    names = [name, 'errors']
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self):

        if self.options.task is None:
            return CommandResult(1, 'ERROR: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Looking up detailed postmortem of task %s' % self.cachedinfo['RequestName'])
        dictresult, postmortem, reason = server.get(self.uri + self.cachedinfo['RequestName'])

        if postmortem != 200:
            msg = "Problem retrieving postmortem:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        if self.options.verbose or self.options.outputfile:
            self.printVerbose(dictresult, self.options.outputfile, os.path.join(self.requestarea, 'results', 'jobFailures.log'))
        else:
            self.logger.debug("Aggregating job failures")
            groupederrs = self.aggregateFailures(dictresult)
            self.logger.info("List of failures and jobs per each failure: (one job could have more then one failure, one per each step)")
            for hkey in groupederrs:
                ## removing duplicates and sort
                joberrs = list(set(groupederrs[hkey]['jobs']))
                joberrs.sort()
                self.logger.info(' %s jobs failed with error "%s"' %(len(joberrs), groupederrs[hkey]['error']))
                self.logger.info('   (%s)'  %(', '.join([ str(jobid[0]) for jobid in joberrs])) )

        return CommandResult(0, None)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Same as -c/-continue" )

        self.parser.add_option( "-v", "--verbose",
                                action = "store_true",
                                dest = "verbose",
                                default = False,
                                help = "Showing detailed failure reasons for each job" )

        self.parser.add_option( '-o', '--outputfile',
                                action = "store_true",
                                dest = 'outputfile',
                                default = False,
                                help = 'The detailed failures of all jobs will be stored in a file')


    def printVerbose(self, dictresult, store = False, outfile = None):
        alljobs = map(int, dictresult.keys())
        alljobs.sort()
        globalmsg = ''
        for jobid in alljobs:
            globalmsg += "\n*Job %d*\n" % jobid
            allretry = map(int, dictresult[str(jobid)].keys())
            allretry.sort()
            for retry in allretry:
                globalmsg += "   -retry: %d\n" % retry
                globalmsg += "    -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*- \n"
                for step in dictresult[str(jobid)][str(retry)]:
                    globalmsg += "      failure details for step '%s':\n" % step
                    for singlefailure in dictresult[str(jobid)][str(retry)][step]:
                        msg = '        Failure type: %s\n' % singlefailure['type'].strip()
                        msg += '        Detailed err: %s\n' % singlefailure['details']
                        msg += '        Exit code: %s' % singlefailure['exitCode']
                        globalmsg += msg
                globalmsg += "\n    -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*- \n"
        if store:
            open(outfile, 'w').write(globalmsg)
            self.logger.info("Report for job failure reasons has been written into the file '%s'" % outfile)
        else:
            self.logger.info(globalmsg)

    def aggregateFailures(self, dictresult):
        failures = {}
        # this allows to get a sorted list of jobs even in debug print
        alljobs = map(int, dictresult.keys())
        alljobs.sort()
        ## for each job
        for jobid in alljobs:
            jobmsg = "Job %d" % jobid
            ## and just for the last retry of the job
            retry = max( map(int, dictresult[str(jobid)].keys()) )
            jobmsg += " failed at trial %d" % retry
            ## and for each step
            for step in dictresult[str(jobid)][str(retry)]:
                jobmsg += " in %s step" % step
                stepfailures = ''
                ## and for each failure of it
                for singlefailure in dictresult[str(jobid)][str(retry)][step]:
                    stepfailures += '"' + singlefailure['type'].strip() + '" - '
                    ## now we create a dict with the hash of the error messsage as main key
                    errtypehash = hashlib.md5( singlefailure['type'].strip() ).hexdigest()
                    if errtypehash in failures:
                        failures[errtypehash]['jobs'].append( (jobid, retry) )
                    else:
                        failures[errtypehash] = {'error': singlefailure['type'].strip(), 'jobs': [(jobid, retry)]}
                jobmsg += " due to %s" % stepfailures[:-1]
            self.logger.debug(jobmsg)
        #{ 'hash': {'error': 'erromsg', 'jobs': [(1, 1),(2,1),(3,2)]}
        return failures
