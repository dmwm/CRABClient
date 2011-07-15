from Commands import CommandResult
from Commands.SubCommand import SubCommand
from ServerInteractions import HTTPRequests


class status(SubCommand):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """

    ## name should become automatically generated
    name  = "status"
    usage = "usage: %prog " + name + " [options] [args]"


    def __call__(self):

        if self.options.task is None:
            return CommandResult(1, 'Error: Task option is required')

        server = HTTPRequests(self.cachedinfo['Server'] + ':' + str(self.cachedinfo['Port']))

        self.logger.debug('Looking up detailed status of task %s' % self.cachedinfo['RequestName'])
        dictresult, status, reason = server.get(self.uri + self.cachedinfo['RequestName'])

        self.logger.debug("Result: %s" % dictresult)

        if status != 200:
            msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(self.cachedinfo['RequestName']), str(dictresult), str(reason))
            return CommandResult(1, msg)

        self.logger.info("Task Status:        %s"    % str(dictresult['requestDetails'][unicode('RequestStatus')]))
        self.logger.info("Completed at level: %s%% " % str(dictresult['requestDetails']['percent_success']))

        for state in dictresult['states']:
            count = dictresult['states'][state]['count']
            jobList = self.readableRange(dictresult['states'][state]['jobs'])
            self.logger.info("State: %s\tCount: %s\tJobs: %s" % (state, count, jobList))

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
