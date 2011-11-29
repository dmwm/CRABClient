from __future__ import division # I want floating points

from CRABClient.Commands import CommandResult
from CRABClient.Commands.SubCommand import SubCommand
from CRABClient.ServerInteractions import HTTPRequests

class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    name  = __name__.split('.').pop()
    names = [name, 'stat']
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self):

        if self.options.task is None:
            return CommandResult(2001, 'ERROR: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri + self.cachedinfo['RequestName'])

        #self.logger.debug("Result: %s" % dictresult)
        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        for workflow in dictresult["workflows"]:
            self.logger.info('#%s %2s' % (workflow['subOrder'], workflow['request']) )
            self.logger.info("   Task Status:        %s"    % str(workflow['requestDetails']['RequestStatus']))
            self._printRequestDetails(workflow)

            if 'states' in workflow:
                ## grouping the status by task, since we may have log collect, cleanup, analysis jobs
                for wmtask in workflow['states'].keys():
                    self.logger.info('   %s jobs' % wmtask.split(self.cachedinfo['RequestName'] + '/', 1)[-1] )
                    totalJobs = 0
                    for state in workflow['states'][wmtask]:
                        totalJobs += workflow['states'][wmtask][state]['count']
                    for state in workflow['states'][wmtask]:
                        count = workflow['states'][wmtask][state]['count']
                        if self.options.brief:
                            percent = count/totalJobs*100
                            self.logger.info("     State: %-13s Count: %6s (%5.1f%%)" % (state, count, percent))
                        else:
                            jobList = self.readableRange(workflow['states'][wmtask][state]['jobs'])
                            self.logger.info("     State: %-13s Count: %6s  Jobs: %s" % (state, count, jobList))

        return CommandResult(0, None)


    def _printRequestDetails(self, dictresult):
        """
        Print the RequestMessages list when the task is failed
        """
        if dictresult.has_key('requestDetails') and \
                  dictresult['requestDetails'][u'RequestStatus'] == 'failed' and \
                  dictresult['requestDetails'].has_key(u'RequestMessages'):
            for messageL in dictresult['requestDetails'][u'RequestMessages']:
                #messages are lists
                for message in messageL:
                    self.logger.info("   Server Messages:")
                    self.logger.info("   \t%s" % message)


    def setOptions(self):
        """
        __setOptions__

        This allows to set specific command options
        """
        self.parser.add_option( "-t", "--task",
                                 dest = "task",
                                 default = None,
                                 help = "Same as -c/-continue" )

        self.parser.add_option( "-b", "--brief",
                                 dest = "brief",
                                 action = "store_true",
                                 default = False,
                                 help = "Provide just a summary of the status" )


    def readableRange(self, jobArray):
        """
        Take array of job numbers and concatenate 1,2,3 to 1-3
        return string
        """
        def readableSubRange(subRange):
            """
            Return a string for each sub range
            """
            if len(subRange) == 1:
                return "%s" % (subRange[0])
            else:
                return "%s-%s" % (subRange[0], subRange[len(subRange)-1])

        # Sort the list and generate a structure like [[1], [4,5,6], [10], [12]]
        jobArray.sort()

        previous = jobArray[0]-1
        listOfRanges = []
        outputJobs = []
        for job in jobArray:
            if previous+1 == job:
                outputJobs.append(job)
            else:
                listOfRanges.append(outputJobs)
                outputJobs = [job]
            previous = job
        if outputJobs:
            listOfRanges.append(outputJobs)

        # Convert the structure to a readable string
        return ','.join([readableSubRange(x) for x in listOfRanges])
