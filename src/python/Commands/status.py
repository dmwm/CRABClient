from Commands import CommandResult
from client_utilities import loadCache
import json

def status(logger, configuration, server, options, requestname, requestarea):
    """
    Query the status of your tasks, or detailed information of one or more tasks
    identified by -t/--task option
    """
    uri = '/crabinterface/crab/task/'
    cachedinfo = loadCache(requestarea)
    logger.debug('Looking up detailed status of task %s' % cachedinfo['RequestName'])
    dictresult, status, reason = server.get(uri + cachedinfo['RequestName'])

    logger.debug("Result: %s" % dictresult)

    if status != 200:
        msg = "Problem retrieving status:\ninput:%s\noutput:%s\nreason:%s" % (str(userdefault), str(dictresult), str(reason))
        return CommandResult(1, msg)
 
    logger.info("Task Status:        %s"    % str(dictresult[unicode('RequestStatus')]))
    logger.info("Completed at level: %s%% " % str(dictresult['percent_success']))
    

    return CommandResult(0, None)
